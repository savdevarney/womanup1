import os

from airtable import Airtable

import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from xml.etree import ElementTree
from pprint import pprint


vote_key = os.environ['VOTE_SMART_API_KEY']
air_key = os.environ['AIR_TABLE_API_KEY']
woman_up = os.environ['AIR_TABLE_WOMANUP']
year = '2018'
previous_year = '2017'

# VOTE SMART END POINTS

base_vote_url = 'http://api.votesmart.org'
states_url = base_vote_url + '/State.getStateIDs?key=' + vote_key
elections_state_year_url = base_vote_url + '/Election.getElectionByYearState?key=' + vote_key
candidates_election_url = base_vote_url + '/Candidates.getByElection?key=' + vote_key
candidate_bio_url = base_vote_url + '/CandidateBio.getBio?key=' + vote_key
categories_url = base_vote_url + '/Rating.getCategories?key=' + vote_key
sig_url = base_vote_url + '/Rating.getSig?key=' + vote_key
ratings_url = base_vote_url + '/Rating.getSigRatings?key=' + vote_key
candidate_ratings_url = base_vote_url + '/Rating.getCandidateRating?key=' + vote_key

# WOMANUP TABLES

states_table = Airtable(woman_up, 'states', air_key)
elections_table = Airtable(woman_up, 'elections', air_key)
running_table = Airtable(woman_up, 'running', air_key)
elected_table = Airtable(woman_up, 'elected', air_key)
category_table = Airtable(woman_up, 'categories', air_key)
sigs_table = Airtable(woman_up, 'sigs', air_key)
ratings_table = Airtable(woman_up, 'ratings', air_key)
scores_table = Airtable(woman_up, 'scores', air_key)
rating_categories = Airtable(woman_up, 'rating_categories', air_key)

# GENERIC SESSION REQUEST

def get_request(url, params=''):
    s = requests.Session()
    retries = Retry(total=5,
                    backoff_factor=0.1,
                    status_forcelist=[ 500, 502, 503, 504 ])
    s.mount('http://', HTTPAdapter(max_retries=retries))
    r = s.get(url, params=params)
    return r

# RETRIEVAL METHODS FOR ITERATORS

def get_state_ids():
    states = []
    state_records = states_table.get_all()
    for state in state_records:
        state_id = state['fields']['stateId']
        states.append(state_id)
    return states

def get_sig_ids():
    sigs = []
    sig_records = sigs_table.get_all()
    for sig in sig_records:
        sig_id = sig['fields']['sigId']
        sigs.append(sig_id)
    return sigs

def get_rating_ids():
    ratings = []
    ratings_records = ratings_table.get_all()
    for rating in ratings_records:
        rating_id = rating['fields']['ratingId']
        ratings.append(rating_id)
    return ratings

def get_candidate_ids():
    candidates = []
    candidates_records = running_table.get_all()
    for candidate in candidates_records:
        candidate_id = candidate['fields']['candidateId']
        candidates.append(candidate_id)
    return candidates

# ITERATORS

states = get_state_ids()
sigs = get_sig_ids()
ratings = get_rating_ids()


# MOSTLY STATIC, SEED VERY OCCASSIONALLY

def state_seed():
    r = requests.get(states_url)
    root = ElementTree.fromstring(r.content)
    for state in root.iter('state'):
        state_id = state.find('stateId').text
        name = state.find('name').text
        states_table.insert({'stateId': state_id, 'name': name})

def category_seed():
    r = requests.get(categories_url)
    root = ElementTree.fromstring(r.content)
    for category in root.iter('category'):
        category_id = category.find('categoryId').text
        category_name = category.find('name').text
        category_table.insert(
            {'categoryId': category_id,
             'name': category_name,
            })

# DYNAMIC, SEED OFTEN (weekly)

def sig_seed(sig_id):
    params = {'sigId': sig_id}
    r = get_request(sig_url, params)
    root = ElementTree.fromstring(r.content)
    sig_id = root.find('sigId').text
    state_id = root.find('stateId').text
    name = root.find('name').text
    description = root.find('description').text
    print('inserting sig info for ' + sig_id)
    sigs_table.insert({
        'sigId': sig_id,
        'stateId': state_id,
        'name': name,
        'description': description,
    })
    # update local store so don't repeat seed
    sigs.append(sig_id)

def rating_seed(sig_id):
    params = {'sigId': sig_id}
    r = get_request(ratings_url, params)
    root = ElementTree.fromstring(r.content)
    for rating in root.iter('rating'):
        rating_id = rating.find('ratingId').text
        time = rating.find('timespan').text
        name = rating.find('ratingName').text
        text = rating.find('ratingText').text
        print('inserting rating info for ' + rating_id)
        ratings_table.insert({
            'ratingId': rating_id,
            'timespan': time,
            'ratingName': name,
            'ratingText': text,
            'sigId': sig_id,
        })
        # update local store so don't repeat seed
        ratings.append(rating_id)

def candidate_ratings_seed():
    candidate_ids = get_candidate_ids()
    for candidate_id in candidate_ids:
        print('getting ratings for candidateId ' + candidate_id)
        params = {'candidateId': candidate_id}
        r = get_request(candidate_ratings_url, params)
        root = ElementTree.fromstring(r.content)
        parent_ratings = root.findall('rating')
        if parent_ratings: 
            for rating in parent_ratings:
                # return xml has nested 'rating' objects.
                # so use findall which only finds objects that are direct children of root.
                    
                # prepare to seed ratings for the candidate
                score = rating.find('rating').text
                name = rating.find('ratingName').text
                text = rating.find('ratingText').text
                sig_id = rating.find('sigId').text
                rating_id = rating.find('ratingId').text
                time = rating.find('timespan').text

                # seed info for recent ratings / sigs
                if previous_year in time or year in time:
                    # if it's a new sig, seed the information
                    if sig_id not in sigs:
                        sig_seed(sig_id)

                    # if it's a new rating, seed the information
                    if rating_id not in ratings:
                        rating_seed(sig_id)
                        
                        # also seed the categories associated with the new rating
                        # note, there are several duplicate categories for a single rating in the vote smart data
                        # will have to clean up in our database ... 
                        categories = rating.find('categories')
                        for category in categories.iter('category'):
                            category_id = category.find('categoryId').text
                            rating_categories.insert({
                                'ratingId' : rating_id,
                                'categoryId': category_id,
                            })

                    # write the candidate's rating info
                    print('inserting ratings for candidateId ' + candidate_id)
                    scores_table.insert({
                        'ratingId': rating_id,
                        'sigId': sig_id,
                        'candidateId': candidate_id,
                        'score': score,
                        'name': name,
                        'text': text,
            })

def election_seed():
    for state in states:
        print('getting elections for state ' + state)
        params = { 'stateId': state, 'year': year }
        r = get_request(elections_state_year_url, params)
        root = ElementTree.fromstring(r.content)
        for election in root.iter('election'):
            election_id = election.find('electionId').text
            election_name = election.find('name').text
            elections_table.insert({
                'electionId': election_id,
                'name': election_name,
                'stateId': state,
                })

def candidate_seed():
    elections = elections_table.get_all()
    for election in elections:
        election_id = election['fields']['electionId']
        print('getting candidates for electionId ' + election_id)
        params = { 'electionId': election_id }
        r = get_request(candidates_election_url, params)
        root = ElementTree.fromstring(r.content)
        for candidate in root.iter('candidate'):
            # get information from election profile
            candidate_id = candidate.find('candidateId').text
            first_name = candidate.find('firstName').text
            last_name = candidate.find('lastName').text
            election_parties = candidate.find('electionParties').text
            election_status = candidate.find('electionStatus').text
            election_stage = candidate.find('electionStage').text
            election_office = candidate.find('electionOffice').text
            election_office_id = candidate.find('electionOfficeId').text
            election_state_id = candidate.find('electionStateId').text
            election_office_type_id = candidate.find('electionOfficeTypeId').text
            election_date = candidate.find('electionDate').text
            photo = candidate.find('photo').text
            
            # get information from detailed bio
            print('getting candidate detailed bio for electionId ' + candidate_id)
            params = { 'candidateId': candidate_id }
            r = get_request(candidate_bio_url, params)
            root = ElementTree.fromstring(r.content)
        
            is_female = root.find('candidate').find('gender').text == 'Female'

            # if female, seed to database
            if is_female(root):
                running_table.insert({
                    'candidateId' : candidate_id,
                    'photo': photo,
                    'firstName' : first_name,
                    'lastName' : last_name,
                    'electionParties' : election_parties,
                    'electionStatus' : election_status,
                    'electionStage' : election_stage,
                    'electionOffice' : election_office,
                    'electionOfficeId' : election_office_id,
                    'electionStateId' : election_state_id,
                    'electionOfficeTypeId' : election_office_type_id,
                    'electionDate': election_date,
                    })
            
                # get office data from detailed bio
                office = root.find('office')
                if (office):
                    office_parties = office.find('officeParties').text
                    title = office.find('title').text # 'Senator'
                    name = office.find('name').text # 'U.S. Senate'
                    first_elect = office.find('firstElect').text
                    next_elect = office.find('nextElect').text # 2018
                    term_start = office.find('termStart').text # 11/10/1992
                    term_end = office.find('termEnd').text
                    district = office.find('district').text # 'Senior Seat'
                    district_id = office.find('districtId').text # 20496
                    state_id = office.find('stateId').text
                    status = office.find('status').text # 'active'

                    elected_table.insert({
                        'candidateId' : candidate_id,
                        'officeParties' : office_parties,
                        'title' : title,
                        'name' : name,
                        'firstElect' : first_elect,
                        'nextElect' : next_elect,
                        'termStart' : term_start,
                        'termEnd' : term_end,
                        'district' : district,
                        'districtId' : district_id,
                        'stateId': state_id,
                        'status' : status,
                    })
            

# TODO: create contact table and seed via separate method

# HOW TO REGULARLY UPDATE WOMAN UP DATABASE FOR NEW ELECTIONS, CANDIDATES AND/OR SCORES:

# 1. clear these tables via air table: elections, running, elected, sigs, ratings, rating_categories, scores
# 2. remove comments from these functions and 'run python seed.py' in terminal
election_seed()
candidate_seed()
candidate_ratings_seed()




