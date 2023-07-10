
# Building an Java Matching Engine - WORK IN PROGRESS

## Requirements:

You should try to establish an understanding of how an Orderbook functions before attempting implementation as this will greatly help you implement the matching logic for this task.

Your task is to implement an orderbook with the following requirements:

### Accept and Match Orders

- Resting orders on the orderbook, if bid/ask prices do not cross.
- Matching orders if bid/ask prices do cross. 
- Returns orderId and status (```RESTING```, ```PARTIAL```, ```FILLED```)

### Cancel Orders
- Removing orders from the orderbook via Order ID
- Returns orderId and status (```CANCELLED```, ```NONE```)

### Tests
- Each business scenario should have corresponding tests.
- The base required tests are in ```OrderbookTest```, you should add more coverage if required.

### Notes
- Majority of implemention will be in ```OrderbookImpl```, however you may have to create additional classes.
- In a applied used case, we may expect high throughput of orders therefore you should use optimal data structures for searching and sorting.  


## Starter Topics:
- Matching Engines & Orderbooks

## Bonus Topics:
- Method that returns sorted bids (price descending) and asks (price ascending).
- Order Generator (Continually generates random orders to be placed into Orderbook).
- Space and Time Complexity Optimisation.


## Documentation:

- [Matching Engine and Orderbooks](https://weareadaptive.atlassian.net/wiki/spaces/KB/pages/3709566997/Exchanges+Matching+Engines#Orders)