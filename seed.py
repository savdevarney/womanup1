import os
from airtable import Airtable
import us
import requests
from xml.etree import ElementTree

vote_key = os.environ['VOTE_SMART_API_KEY']
air_key = os.environ['AIR_TABLE_API_KEY']
table_name = 'Women Up'
year = '2018'

base_vote_url = 'http://api.votesmart.org'
elections_state_year_url = base_vote_url + '/Election.getElectionByYearState?key=' + vote_key
candidates_election_url = base_vote_url + '/Candidates.getByElection?key=' + vote_key


# list of all 50 states [<State: Alabama>, <State:Alaska> ...]
all_states = us.states.STATES

# loop through all states, get all elections
for state in all_states:
    abbr = state.abbr
    # get_state_election_ids(abbr)

def get_state_election_ids(state):
    election_ids = []
    params = { 'year': year, 'stateId': state }
    headers = { 'Content-Type': 'application/json' }
    r = requests.get(elections_state_year_url, params=params, headers=headers)
    root = ElementTree.fromstring(r.content)
    for election_id in root.iter('electionId'):
        election_ids.append(election_id.text)
    return election_ids

def get_election_candidates(electionIdList):
    candidate_ids = []
    for election_id in election_id_list: 
        params = { 'electionId': electionId }
        headers = { 'Content-Type': 'application/json' }
        r = requests.get(candidates_election_url, params=params, headers=headers)
        