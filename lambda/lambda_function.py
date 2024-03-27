import base64
import json
import os
import logging
import uuid
import boto3

sm_endpoint_name = os.getenv('SM_ENDPOINT_NAME', "j2-jumbo-instruct")
down_dict = {1: "first down", 2: "second down", 3: "third down", 4: "forth down"}
quarter_dict = {1: "first quarter", 2: "second quarter", 3: "third quarter", 4: "forth quarter", 5: "overtime"}
styles = ["NFL", "tweeter", "poetic"]
languages = ["English", "Spanish", "German"]
cols = [ 'yrdln', 'time', 'qtr', 'total_home_score', 'total_away_score', 'play_type',
     'posteam', 'defteam', 'down', 'yards_gained', 'drive', 'sack', 'complete_pass',
     'penalty', 'penalty_team', 'penalty_player_name', 'penalty_yards', 'penalty_type',
     'kick_distance', 'return_yards', 'kicker_player_name', 'kickoff_returner_player_name',
     'pass_length', 'ydstogo','passer_player_name', 'receiver_player_name', 'rusher_player_name',
     'solo_tackle_1_player_name', 'assist_tackle_1_player_name', 'punter_player_name',
     'field_goal_result', 'extra_point_result', 'touchdown'
]
kinesis_data_stream_key = "SPORT_DATA_LIVE_COMMENTARIES_STREAM"
kinesis_data_stream = os.getenv(kinesis_data_stream_key, "sports-data-live-commentaries")
#bedrock_model_id = "ai21.j2-jumbo-instruct"
bedrock_model_id = "ai21.j2-ultra-v1"

def get_bedrock_client():
    if "ASSUMABLE_ROLE_ARN" in os.environ:
        session = boto3.Session()
        sts = session.client("sts")
        response = sts.assume_role(
            RoleArn=os.environ.get("ASSUMABLE_ROLE_ARN", None),
            RoleSessionName="bedrock"
        )
        new_session = boto3.Session(aws_access_key_id=response['Credentials']['AccessKeyId'],
                              aws_secret_access_key=response['Credentials']['SecretAccessKey'],
                              aws_session_token=response['Credentials']['SessionToken'])

        bedrock = new_session.client('bedrock-runtime' , 'us-east-1')
    else:
        bedrock = boto3.client("bedrock-runtime", "us-east-1")
    return bedrock

boto3_bedrock = get_bedrock_client()

def generate_prompts(row):
    """
    Prompt generator based on the given row.
    :param row: dictionary that describes the play.

    :return: curated prompt based on the given row.
    """

    prompt = {}
    prompt['starting_yard_line'] = row['yrdln']
    prompt['quarter_time_remaining'] = row['time']
    prompt['home_team_score'] = row['total_home_score']
    prompt['away_team_score'] = row['total_away_score']
    play_type = row['play_type']
    offensive_team = row['posteam']
    defensive_team = row['defteam']

    if str(row['down']).isnumeric():
        down = down_dict[int(row['down'])]
    yards_gained = row['yards_gained']
    if row['time'] == '15:00':
        prompt['new quarter'] = True
    if row['touchdown'] == 1:
        prompt['touchdown'] = "touchdown"
        prompt['number of drive'] = row['drive']
    if row['sack'] == 1:
        prompt['sacked'] = 'sacked'

    if row['penalty'] == 1:
        prompt['is penalty'] = True
        prompt['penalty team'] = row['penalty_team']
        prompt['penalty player'] = row['penalty_player_name']
        prompt['penalty yards'] = row['penalty_yards']
        prompt['penalty type'] = row['penalty_type']
    if play_type == "kickoff":
        prompt['quarter'] = quarter_dict[int(row['qtr'])]
        prompt['play_type'] = play_type
        prompt['team'] = defensive_team
        prompt['receiving team'] = offensive_team
        prompt['distance'] = row['kick_distance']
        prompt['return yards'] = row['return_yards']
        prompt['player name'] = row['kicker_player_name']
        prompt['returner player name'] = row['kickoff_returner_player_name']

    elif play_type == "pass":
        prompt['play_type'] = play_type
        prompt['offensive_team'] = offensive_team
        prompt['defensive_team'] = defensive_team
        prompt['down'] = down
        prompt['pass_length'] = row['pass_length']
        if row['touchdown'] != 1:
            prompt[f"{down} with yards to go"] = row['ydstogo']
        prompt['passer_player_name'] = row['passer_player_name']
        prompt['receiver_player_name'] = row['receiver_player_name']
        prompt['passing_yards_gained'] = yards_gained
        if row['complete_pass'] == 1:
            prompt['completion'] = "complete pass"
        else:
            prompt['completion'] = "incomplete pass"
    elif play_type == "run":
        prompt['play type'] = play_type
        prompt['offensive_team'] = offensive_team
        prompt['defensive_team'] = defensive_team
        prompt['rusher_player_name'] = row['rusher_player_name']
        prompt['rushing_yards_gained'] = yards_gained
        prompt['tackle_1_player_name'] = row['solo_tackle_1_player_name']
        prompt['tackle_2_player_name'] = row['assist_tackle_1_player_name']
    elif play_type == "punt":
        prompt['play_type'] = play_type
        prompt[f"{down} with yards to go"] = row['ydstogo']
        prompt['offensive_team'] = offensive_team
        prompt['defensive_team'] = defensive_team
        prompt['punt_distance'] = row['kick_distance']
        prompt['punter_player_name'] = row['punter_player_name']
    elif play_type == "field_goal":
        prompt['play_type'] = play_type
        prompt['offensive_team'] = offensive_team
        prompt['defensive_team'] = defensive_team
        prompt[f"{down} with yards to go"] = row['ydstogo']
        prompt['field_goal_result'] = row['field_goal_result']
        prompt['kick_distance'] = row['kick_distance']
        prompt['kicker_player_name'] = row['kicker_player_name']
    elif play_type == "extra_point":
        prompt['offensive_team'] = offensive_team
        prompt['defensive_team'] = defensive_team
        prompt['extra_point_result'] = row['extra_point_result']
        prompt['kicker_player_name'] = row['kicker_player_name']

    elif play_type != play_type:  # nan
        prompt['offensive_team'] = offensive_team
        prompt['defensive_team'] = defensive_team
        prompt['end_of_the_quarter'] = True

    prompt_objs = []
    for style in styles:
        for language in languages:
            prompt_obj = {}
            prompt_obj['language'] = language
            prompt_obj['style'] = style
            prompt_str = f"{json.dumps(prompt)} \n As a professional sportscaster, write a {style} style commentary in {language} using 2 sentences"
            prompt_obj['prompt'] = prompt_str
            prompt_objs.append(prompt_obj)
    return prompt_objs


def generate_commentary(prompt):
    """
    Returns the generated commentary for a given prompt.
    :param prompt: prompt that gets fed into the model for commentary generation.
    :return: generated text
    """
    global boto3_bedrock

    request = {
        "prompt": prompt,
        "maxTokens":50,
        "temperature":0,
        "topP":1.0,
        "stopSequences":[],
        "countPenalty": {"scale":0},
        "presencePenalty":{"scale":0},
        "frequencyPenalty":{"scale":0}
    }
    body = json.dumps(request)
    content_type = "application/json"
    response = boto3_bedrock.invoke_model(body=body, modelId=bedrock_model_id, accept="*/*",
                                          contentType=content_type)
    response_body = json.loads(response.get('body').read())
    commentary_text = response_body['completions'][0]['data']['text'][1:]
    return commentary_text


def get_commentaries(row):
    """
    Create commentaries from the input data.

    :param row: data ingested from the stream
    :return: commentary objects
    """
    prompt_objs = generate_prompts(row)
    generated_commentary_objs = []
    for prompt_obj in prompt_objs:
        commentary_obj = {}
        generated_commentary = f"({row['time']}) {generate_commentary(prompt_obj['prompt'])}"
        commentary_obj['commentary'] = generated_commentary
        commentary_obj['style'] = prompt_obj['style']
        commentary_obj['language'] = prompt_obj['language']
        commentary_obj['prompt'] = prompt_obj['prompt']
        generated_commentary_objs.append(commentary_obj)

    return generated_commentary_objs


def get_row_data(row):
    """
    Extract relevant columns from the input data.
    :param row: input data
    :return: data filtered by the given column names
    """
    df_json = {}
    for col in cols:
        df_json[col] = row[col]
    return df_json


class KinesisStream(object):

    def __init__(self, stream):
        self.stream = stream

    def _connected_client(self):
        """ Connect to Kinesis Streams """
        return boto3.client('kinesis')

    def send_stream(self, data, partition_key=None):
        """
        data: python dict containing your data.
        partition_key:  set it to some fixed value if you want processing order
                        to be preserved when writing successive records.

                        If your kinesis stream has multiple shards, AWS hashes your
                        partition key to decide which shard to send this record to.

                        Ignore if you don't care for processing order
                        or if this stream only has 1 shard.

                        If your kinesis stream is small, it probably only has 1 shard anyway.
        """

        # If no partition key is given, assume random sharding for even shard write load
        if partition_key == None:
            partition_key = str(uuid.uuid4())

        client = self._connected_client()
        return client.put_record(
            StreamName=self.stream,
            Data=json.dumps(data),
            PartitionKey=partition_key
        )


kinesis = KinesisStream(kinesis_data_stream)


def lambda_handler(event, context):
   print("entering lambda_handler")

   for record in event['Records']:
       payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
       print("Decoded payload: " + payload)
       data = json.loads(payload)
       row = get_row_data(data)
       commentary_objs = get_commentaries(row)
       for commentary_obj in commentary_objs:
           print(f"commentary: {commentary_obj['commentary']}")
       commentary_row_objs = {}
       commentary_row_objs['commentary_objs'] = commentary_objs
       commentary_row_objs['row'] = row
       if 'sess_id' in data:
           print(f"found session ID:{data['sess_id']}")
           commentary_row_objs['sess_id'] = data['sess_id']
           kinesis.send_stream(commentary_row_objs)

   return 'Successfully processed {} records.'.format(len(event['Records']))
    
