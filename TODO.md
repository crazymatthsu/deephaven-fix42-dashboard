[deephaven FIX trading Dashboard]

- the project '/Users/maojenhsu/ai-code/fix42-oms-cache/' is to build a FIX 42 state machine cache for latest order state cache. 
- take a look at the docs folders under below folders, it has analysis of the FIX 42 messages, and how to link different FIX messages together to maintain the latest state of each order.
    - /Users/maojenhsu/ai-code/fix42-oms-cache/claude-code/docs/
    - /Users/maojenhsu/ai-code/fix42-oms-cache/grok/docs/
- for our deephaven FIX trading Dashboard, it will 
    - receive FIX 42 messages (35=D,G,F,8,9,Q) from Kafka 
    - do analysis of what are the types of deephaven tables shoudl be used in this project to achieve building a FIX 42 state machine cache for latest order state cache.
    - what's the DAG structure shoudl be in deephaven ? 
    - do analysis of deephaven features , api , to help us to build this dashboard.
    - the state machine needs to handle the following scenarios:
        - new order ack/reject
        - amend order ack/reject
        - cancel order ack/reject 
        - execution report new/partial fill/full fill/amend fill/cancel fill
        - cancel reject
        - don't know trade
    - visualize the latest state of each order in real-time
    - provide a FIX 42 state machine cache API to query the latest state of each order by Account, Symbol, ClOrdID, OrderID, or ExecID.
    - create a dashboard to visualize the latest state of the orders, click on an order to see executions in a different panels, and order new/amend/cancel history in another panel 

[project structure]
- root level has a ./TODO.md 
- ./claude-code/ : this will be used only by claude code , and claude code should not look at other folders used by other AI models to avoid confusion.
  - ./claude-code/docs/ : contains design and analysis .md files 
  - there will be submodules under ./claude-code/ for different components of the project, for both java and python code.
  - for python code, follow deephaven server side scripting best practices
- ./grok/ : this will be used only by grok code only, and grok code should not look at other folders used by other AI models to avoid confusion.
  - ./grok/docs/ : contains design and analysis .md files
  - there will be submodules under ./grok/ for different components of the project, for both java and python code.
  - for python code, follow deephaven server side scripting best practices 
- use gradle to build the project 
- use java 21 as programming language
- for deephaven python scripts, group them under a different submodule. 

[demo]
- create mock FIX42 messages and feed them into deephaven via Kafka
- use deephaven with kafka docker image and run it locally via podman desktop 
- create a deephaven dashboard to visualize the latest state of each order, and click on an order to see executions in a different panels, and order new/amend/cancel history in another panel

[unit test and integration test]
- create unit test for each component of the project, and integration test for the whole project.