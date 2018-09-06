import os
from airtable import Airtable
import requests
from xml.etree import ElementTree
from pprint import pprint

vote_key = os.environ['VOTE_SMART_API_KEY']
air_key = os.environ['AIR_TABLE_API_KEY']
woman_up = os.environ['AIR_TABLE_WOMANUP']
year = '2018'

base_vote_url = 'http://api.votesmart.org'
states_url = base_vote_url + '/State.getStateIDs?key=' + vote_key
elections_state_year_url = base_vote_url + '/Election.getElectionByYearState?key=' + vote_key
candidates_election_url = base_vote_url + '/Candidates.getByElection?key=' + vote_key
candidate_bio_url = base_vote_url + '/CandidateBio.getBio?key=' + vote_key

# WOMANUP TABLES
states_table = Airtable(woman_up, 'states', air_key)
elections_table = Airtable(woman_up, 'elections', air_key)
candidates_table = Airtable(woman_up, 'candidates', air_key)


def is_candidate_female(candidate_id):
    params = { 'candidateId': candidate_id }
    r = requests.get(candidate_bio_url, params=params)
    root = ElementTree.fromstring(r.content)
    return root.find('candidate').find('gender').text == 'Female'


def state_seed():
    r = requests.get(states_url)
    root = ElementTree.fromstring(r.content)
    for state in root.iter('state'):
        state_id = state.find('stateId').text
        name = state.find('name').text
        states_table.insert({'stateId': state_id, 'name': name})


def election_seed():
    states  = states_table.get_all()
    for state in states:
        state_id = state['fields']['stateId']
        params = { 'stateId': state_id, 'year': year }
        r = requests.get(elections_state_year_url, params=params)
        root = ElementTree.fromstring(r.content)
        for election in root.iter('election'):
            election_id = election.find('electionId').text
            election_name = election.find('name').text
            elections_table.insert({'electionId': election_id, 'name': election_name, 'stateId': state_id})


def candidate_seed():
    elections = elections_table.get_all()
    for election in elections:
        election_id = election['fields']['electionId']
        params = { 'electionId': election_id }
        r = requests.get(candidates_election_url, params=params)
        root = ElementTree.fromstring(r.content)
        for candidate in root.iter('candidate'):
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
            if is_candidate_female(candidate_id):
                candidates_table.insert(
                    {'candidateId' : candidate_id,
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






