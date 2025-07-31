from google.adk.agents import Agent, SequentialAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools.tool_context import ToolContext
from google.genai import types
from google.cloud import bigtable
from google.cloud.bigtable.row_set import RowSet

import google.auth
import json

APP_NAME = "phone_activity_app"
USER_ID = "1234"
SESSION_ID = "session1234"
MODEL = "gemini-2.0-flash"

PROJECT_ID="qwiklabs-asl-01-e660751acd56"
BT_INSTANCE_ID="phonelogs"
BT_TABLE_ID="phone_user_activity"

##### TOOL FOR MAIN AGENT
def get_phone_logs(patient_id: str, start_time: int, end_time: int, recordType: str, tool_context: ToolContext):
    """
    Retrieves phone user activity data for a given patient id within a time range. In BT, UserActivityRecord is also known as phone user activity or phone logs. Do NOT do anything if 'phone_logs' has been populated and the user has not provided a new patient_id.
    
    Args:
        patient_id (str): a UUID for a given patient
        start_time (int): a timestamp in unix seconds
        end_time (int): a timestamp in unix seconds
        recordType (str): a label for the record type
        tool_context (dict): The agent's ToolContext, which contains session state, automatically injected by the runner.
        
    Returns:
        string: JSON-formatted data of phone user activity records, or None if an error occurs.
    """
    
    client = bigtable.Client(project=PROJECT_ID)
    instance = client.instance(BT_INSTANCE_ID)
    table = instance.table(BT_TABLE_ID)

    start_key = f"{patient_id}#{recordType}#{start_time}"
    end_key = f"{patient_id}#{recordType}#{end_time}"
    
    #print(start_key)
    
    column_family_id = "raw"
    column_id = "Raw".encode("utf-8")
    
    row_set = RowSet()
    row_set.add_row_range_from_keys(start_key, end_key)
    
    rows = table.read_rows(row_set=row_set)
    readableRows = []
    for row in rows:
        readableRow = row.cells[column_family_id][column_id][0].value.decode("utf-8")
        #print(readableRow)
        readableRows.append(readableRow)
    
    # Update the state with the results, don't return it.
    tool_context.state["phone_logs"] = readableRows
    return f"Successfully fetched {len(readableRows)} phone log records. They are now available for observation."

##### TOOL FOR OBSERVATION AGENT
def make_observation(tool_context: ToolContext):
    """
    Makes an observation about the 'phone_logs' in state given the user's question. 

    Args:
        tool_context (dict): The agent's ToolContext, which contains session state, automatically injected by the runner.
    Returns:
        string: A string response to the user's question.
    """
    phone_logs = tool_context.state.get("phone_logs")

    if not phone_logs:
        return "Error: No phone logs have been loaded into the cache. Use the 'get_phone_logs' tool first."

    # print(f"DEBUG: Retrieving {len(phone_logs)} logs from state for agent analysis.")
    # Return the logs. The agent will use this output for its reasoning.
    # We join them into a single string to return, as tools return strings.
    return json.dumps(phone_logs)
    

##### SUB AGENT FOR ONLY QUESTIONS ABOUT THE PHONE LOGS
observation_agent = Agent(
    model=MODEL,
    name="observation_agent",
    instruction="You are the general observations agent. You make observations about the 'phone_logs' found in state. Do NOT do anything if 'phone_logs' has not been populated.",
    description="""Handles general observations about the phone logs using the 'make_observation' tool. Contents of a user activity record are json-formatted, and contain the following keys:
    
      "Stream": Enum identifier for the device and operating system,
      "RecordedSystemTime": UTC representation of the recorded display time,
      "RecordedDisplayTime": datetimeoffset expressed as string of the even that occurred on the device,
      "UseractivityType": category of log types,
      "UseractivitySubType": subcategory of UseractivityType, there is a one-to-many mapping of UseractivityType to UseractivitySubType,
      "Data": raw logs of the event expressed as a key-value pair in JSON format,
      "TransmitterNumber": unique identifier for transmitter device,
      "RecordType": UserActivityRecord is the only supported RecordType of the available data
      
      When a user asks for specifics about Transmitters, filter by UseractivityType "Transmitter" or "Display Screen". When the user asks about battery issues, use the UseractivityType "Battery". When the user asks about anything related to the phone OS, use the "OS" useractivitytype. If the user asks to identify any network related logs, use the "Networking" useractivitytype. Start with a general summary of what you see when the data has been returned from the read_agent based off the Data field. Also provide a breakdown with counts of UserActivitySubTypes grouped by UserActivityTypes.
      
    """,
    tools=[make_observation]
)


# Session and Runner
async def setup_session_and_runner():
    initial_state = {"phone_logs": []}  # Store the phone logs we initially query in state.
    session_service = InMemorySessionService()
    session = await session_service.create_session(
        app_name=APP_NAME, 
        user_id=USER_ID, 
        session_id=SESSION_ID,
        state=initial_state
    )
    runner = Runner(agent=root_agent, app_name=APP_NAME, session_service=session_service)
    return session, runner

# Agent Interaction
async def call_agent_async():
    content = types.Content(role='user', parts=[types.Part(text=query)])
    session, runner = await setup_session_and_runner()
    events = runner.run_async(user_id=USER_ID, session_id=SESSION_ID, new_message=content)

    async for event in events:
        if event.is_final_response():
            final_response = event.content.parts[0].text
            print("Agent Response: ", final_response)

            
read_agent = Agent(
    model=MODEL,
    name="read_agent",
    description=(
       "Agent that answers questions about BigTable data by executing row reads."
    ),
    instruction="""You are an agent with access to bigtable. You will be asked to perform a lookup on the data present in there. The user will provide a human-readable date or datetime, you are expected to convert that time into unix seconds. At minimum, the date should contain a day,  month, and year. If no timestamp is provided, assume midnight to 11:59pm. If the user asks for phone logs or user logs, use UserActivityRecord for the recordType.

    """,
    tools=[
       get_phone_logs
    ]
)

##### MAIN AGENT FOR THE ACTUAL QUERY
root_agent = Agent(
    model=MODEL,
    name="main_agent",
    description=(
        "Agent that routes requests"
    ),
    instruction="""You are an agent that has access to a read_agent and an observation_agent. Based off the request, when the user asks for data using a patient, record type, and date range, you should use the read_agent. If the user asks for further analysis of the data, use the observation_agent.""",
    sub_agents=[
        read_agent,
        observation_agent
    ]
)