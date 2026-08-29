
goal: 
this module will implement the issue https://github.com/crazymatthsu/deephaven-fix42-dashboard/issues/10
but not necessarily fully follow the implementation describe in the issue 
claude should explore better way to display on the UI and provide user the ability to reconcile upstream to downstream OMS rollup cumQty, leavesQty, total notional (avg px * cumQty) 

assume there are three different OMS hubs from upstream to downstream, it may have up to 4 OMS hubs FIX messages drop copy into deephaven 

1. OMS-A
   35=D , 11=OMS-A-11
2. OMS-B-parent 
    assume we will see the 35=D, 11=OMS-B-parent-11 drop copy FIX message will have tag 16666="OMS-A-11" ( OMS-A's tag 11) as external order id 
    
3. OMS-B-child 
    one parent can have multiple child 
    assume we will see the child order 35=D, 11=OMS-B-child-11 drop copy FIX message will have its parent order id set at tag 16667="OMS-B-parent-11" as external order id

5. OMS-C
    assume we will see the order 35=D drop copy FIX message will have tag 16668="OMS-B-child-11" as external order id 

the code needs to be able to handle OMS-B's parent and child orders , and needs to be able to configure tag of external order id for linking among upstream and downstream orders 
assume tag 1 on the order will have client orders' account 

the UI needs to allow user to type in or select from drop down of : 
- client's account ( optional )
- symbol ( optional )
- side ( optional )
- source system ( optional )

UI should be smart enough to have paging to avoid overloading front end 

the order blotter should allow users to see all orders from upstream to downstream 
or only focus on one or more source systems

evaluate whether this submodule should be in python only or mix python ui with java FIX state machine ? 

outline the design of DAG in deephaven , how to structure this submodule code and linking of differnet tables among different source systems




