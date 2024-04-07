import gradio as gr
import json
import os
import pandas as pd
import boto3
import logging
import subprocess
import botocore.exceptions
import gc
from threading import Thread
from multiprocessing.managers import BaseManager
import time

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        logging.StreamHandler()
    ]
)
manager = BaseManager(address=('127.0.0.1', 0))
max_lines = 50
default_language = "English"
default_style = "NFL"
cols = [ 'yrdln', 'time', 'qtr', 'total_home_score', 'total_away_score', 'play_type',
     'posteam', 'defteam', 'down', 'yards_gained', 'drive', 'sack',
     'penalty', 'penalty_team', 'penalty_player_name', 'penalty_yards', 'penalty_type',
     'kick_distance', 'return_yards', 'kicker_player_name', 'kickoff_returner_player_name',
     'pass_length', 'ydstogo','passer_player_name', 'receiver_player_name', 'rusher_player_name',
     'solo_tackle_1_player_name', 'assist_tackle_1_player_name', 'punter_player_name',
     'field_goal_result', 'extra_point_result'
]
datatypes = ["str"] * len(cols)
kinesis_client = boto3.client('kinesis')
kinesis_data_stream_key = "SPORT_DATA_LIVE_COMMENTARIES_STREAM"
kinesis_data_stream = os.getenv(kinesis_data_stream_key, "sports-data-live-commentaries")
shard_id = "shardId-000000000000" # assumes 1 shard in the demo
user_states = {}

class RunnableKinesisStreamConsumer(Thread):
    def __init__(self, sess_id):
        # call the parent constructor
        super(RunnableKinesisStreamConsumer, self).__init__()
        self.stop = False
        self.sess_id = sess_id

    def run(self):
        global user_states

        if self.sess_id not in user_states:
            raise Exception("There should already be a user state captured")

        shard_iterator = None
        while True:
            user_state = user_states[self.sess_id]
            if self.stop:
                break
            try:
                if not shard_iterator:
                    response = kinesis_client.get_shard_iterator(
                            StreamName=kinesis_data_stream,
                            ShardId=shard_id,
                            ShardIteratorType='LATEST'
                        )
                    shard_iterator = response['ShardIterator']

                response = kinesis_client.get_records(
                    ShardIterator=shard_iterator,
                    Limit=1
                )
            except botocore.exceptions.ClientError as error:
                if error.response['Error']['Code'] == 'ExpiredIteratorException':
                    print("Iterator expired, create new shard iterator")
                    response = kinesis_client.get_shard_iterator(
                            StreamName=kinesis_data_stream,
                            ShardId=shard_id,
                            ShardIteratorType='LATEST'
                        )
                    shard_iterator = response['ShardIterator']

                    response = kinesis_client.get_records(
                        ShardIterator=shard_iterator,
                        Limit=1
                    )
                else:
                    print("Not handling this exception")
                    raise error

            shard_iterator = response['NextShardIterator']

            records = response['Records']
            if len(records) == 0:
                logging.info("No records found.")
            else:
                json_record = json.loads(records[0]['Data'])
                matching_record = find_commentary(json_record, user_state)
                if 'sess_id' in json_record:
                    if user_state:
                        if user_state['sess_id'] == json_record['sess_id']:
                            if 'generated_commentaries' not in user_state:
                                user_state['generated_commentaries'] = []
                            user_state['generated_commentaries'].append(matching_record)
                            # generated_commentaries.append(matching_record)
                            logging.info(f"matching commentary: {matching_record}")
                            row_data = get_row_data(json_record['row'])
                            df = pd.DataFrame(row_data)
                            if 'display_records' not in user_state:
                                user_state_display_records = pd.DataFrame(columns=cols)
                                user_state['display_records'] = user_state_display_records
                            user_state['display_records'] = pd.concat([user_state['display_records'], df], ignore_index=True)
            time.sleep(2)

def get_row_data(row):
    """
        Converts row data from the kinesis data stream into Json that can be used by Pandas dataframe.
        :param row: input from kinesis data stream
        :return: json format to create a Pandas dataframe
    """

    df_json = {}
    for col in cols:
        df_json[col] = [row[col]]
    return df_json


def find_commentary(json_record, user_state):
    """
        Finds the commentary based on user settings.
        :param json_record: input record from kinesis data stream
        :param user_state: session specific information
        :return: a matching commentary record
    """
    commentary_records = json_record['commentary_objs']
    if 'style' in user_state:
        style = user_state['style']
    else:
        style = default_style

    if 'language' in user_state:
        language = user_state['language']
    else:
        language = default_language

    for record in commentary_records:
        if record['style'] == style and record['language'] == language:
            return record['commentary']

def get_user_session_id(request: gr.Request):
    if 'headers' in request.kwargs:
        if 'cookie' in request.kwargs['headers']:
            cookies_data = request.kwargs['headers']['cookie'].split(";")
            for cookie_data in cookies_data:
                c_key, c_val = cookie_data.split("=")
                if c_key.strip() in ['access-token', "access-token-unsecure"]:
                    access_token = c_val.strip()
                    return access_token
    else:
        if request.request:
            cookies = request.request.cookies
            if "access-token-unsecure" in cookies:
                access_token = cookies["access-token-unsecure"].strip()
            elif "access-token" in cookies:
                access_token = cookies["access-token"].strip()
            return access_token

    return "anonymous"

def get_user_state(sess_id):
    global user_states
    if sess_id in user_states:
        return user_states[sess_id]
    else:
        return {}


def start_simulator(sess_id):
    process = subprocess.Popen(["python3", "live-sports-data-simulator.py", "--sess_id", sess_id])
    return process


def button_simulator_change(user_state, request: gr.Request):
    global user_states
    sess_id = get_user_session_id(request)
    commentaries = " "
    display_records = pd.DataFrame(columns=cols)
    user_state = get_user_state(sess_id)
    if user_state and 'sess_id' in user_state:
        user_state = user_states[user_state['sess_id']]
        if 'generated_commentaries' in user_state:
            generated_commentaries = user_state['generated_commentaries']
            commentaries = "\n".join(generated_commentaries[-max_lines:])
        if 'display_records' in user_state:
            display_records = user_state['display_records'].iloc[-max_lines:]
        if 'simulation_subprocess' not in user_state:
            process = start_simulator(sess_id)
            user_state['simulation_subprocess'] = process
        else:
            process = user_state['simulation_subprocess']
            status_code = process.poll()
            if type(status_code) == int: # the job has returned. Start a new job.
                process = start_simulator(sess_id)
                user_state['simulation_subprocess'] = process

    else:
        user_state['sess_id'] = sess_id
        process = start_simulator(sess_id)
        user_state['simulation_subprocess'] = process

    user_states[user_state['sess_id']] = user_state  # save states for user
    if 'thread' not in user_state:
        t = RunnableKinesisStreamConsumer(user_state['sess_id'])
        t.start()
        user_state['thread'] = t

    return user_state, commentaries, display_records


def comm_change(user_state):
    global user_states

    if 'sess_id' in user_state and user_state['sess_id'] in user_states:
        curr_user_state = user_states[user_state['sess_id']]

        if 'generated_commentaries' in curr_user_state:
            generated_commentaries = curr_user_state['generated_commentaries']
            commentaries = "\n".join(generated_commentaries[-max_lines:])
        else:
            commentaries = " "

        if 'display_records' in curr_user_state:
            display_records = curr_user_state['display_records'].iloc[-max_lines:]
        else:
            display_records = pd.DataFrame(columns=cols)
    else:
        curr_user_state = user_state
        commentaries = " "
        display_records = pd.DataFrame(columns=cols)

    return curr_user_state, commentaries, display_records

def output_df_change(user_state):
    global user_states

    if 'sess_id' in user_state:
        if user_state['sess_id'] in user_states:
            new_user_state = user_states[user_state['sess_id']]
        else:
            new_user_state = user_state
    else:
        new_user_state = user_state

    if 'display_records' in new_user_state:
        display_records = new_user_state['display_records'].iloc[-max_lines:]
    else:
        display_records = pd.DataFrame(columns=cols)
    return new_user_state, display_records

def button_stop_change(user_state, request: gr.Request ):
    global user_states
    session_id = get_user_session_id(request)
    if session_id in user_states:
        cached_user_state = user_states[session_id]
        if 'display_records' in cached_user_state:
            del [cached_user_state['display_records']]
        if 'generated_commentaries' in cached_user_state:
            cached_user_state['generated_commentaries'].clear()
        if 'thread' in cached_user_state:
            thread = cached_user_state['thread']
            thread.stop = True
            cached_user_state.pop('thread')
        if 'simulation_subprocess' in cached_user_state:
                subprocess = cached_user_state['simulation_subprocess']
                subprocess.kill()
                cached_user_state.pop('simulation_subprocess')
        gc.collect()
        # user_states.pop(session_id)
    return user_state, " ", pd.DataFrame(columns=cols)

css = """
#warning {background-color: #FFCCCB} 
.commentary textarea {width: 960px; height: 400px}
"""

with gr.Blocks(css=css) as demo:

    user_state = gr.State({})


    def on_change_language(value, request: gr.Request):
        '''
        Function that changes the language of the commentary.

        :param value: commentary language
        :param request: request from the user session
        :return: changed language value
        '''
        global user_states
        session_id = get_user_session_id(request)
        if session_id in user_states:
            user_state = user_states[session_id]
            user_state['language'] = value

    def on_change_style(value, request: gr.Request):
        '''
        Function that changes the style of the commentary.

        :param value: commentary style
        :param request: request from the user session
        :return: changed style value
        '''
        global user_states
        session_id = get_user_session_id(request)
        if session_id in user_states:
            user_state = user_states[session_id]
            user_state['style'] = value
        else:
            return value

    with gr.Row():
        with gr.Column(scale=1):
            with gr.Row():
                with gr.Column(scale=1):
                    with gr.Row():
                        gr.Markdown(
                            """
                            # Welcome to GenAI Sports Commentary Demo!
                            
                            ### Watch how GenAI model turns live game telemetry data into your favorite commentary!
                            
                            ### LET'S GET STARTED!!
                              
                            """)

            language_radio = gr.Radio(["English", "Spanish", "German"], value="English", label="Language",
                                      info="Translate to target language")
            style_radio = gr.Radio([("Standard", "NFL"), ("X (Tweeter)", "tweeter"), ("Poetic", "poetic")], value="NFL", label="Style",
                                   info="The style of the commentary")
            with gr.Row():
                button_simulator_start = gr.Button(value="Start Game Simulator", variant='primary')
                button_stop = gr.Button(value="Stop", variant='stop')
        with gr.Column(scale=2):
            commentary = gr.TextArea(label="GenAI Live Commentary",
                                     max_lines=max_lines, lines=max_lines, elem_classes="commentary")
    with gr.Row():
        output_df = gr.DataFrame(label="   Live Play by Play Telemetry Data", headers=cols,
                                datatype=datatypes,
                                max_cols=len(cols),
                                max_rows=max_lines,
                                col_count=(len(cols), "dynamic"))

        language_radio.change(on_change_language, inputs=[language_radio], outputs=[], queue=False)
        style_radio.change(on_change_style, inputs=[style_radio], outputs=[], queue=False)
        button_simulator_start.click(button_simulator_change, [user_state], [user_state, commentary, output_df])
        button_stop.click(button_stop_change, [user_state], [user_state, commentary, output_df])
        commentary.change(comm_change, [user_state], [user_state, commentary, output_df], every=3)

demo.queue()
if ("GRADIO_USERNAME" in os.environ) and ("GRADIO_PASSWORD" in os.environ):
  demo.launch(server_name="0.0.0.0", auth=(os.environ['GRADIO_USERNAME'], os.environ['GRADIO_PASSWORD']), share=False)
else:
  demo.launch(server_name="0.0.0.0", share=False)

