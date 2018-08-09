Trustnote rust init project
## Goal
* to pass simple test cases for DAG based block chain
* to supply a basic dev framework for future rust development 

## Supported and Not supported
* nodes discovery is not included(each node would have a fixed peer list)
* only payment unit is supported, which means other messages and functions are not supported in this version

## Methodology
* rewrite subset of JS based Trustnote, no algorithm changed, just language level translation

## Components
all the following components are implemented by RUST.
* network - wss based interfaces
* database - sqlite based storage
* specs - json data serialization/de-serialization for unit
* consensus - DAG algorithm (this would be a big project, need to learning a lot about the current implementation)
* crypto /hash

## Functions need to develop
* catchup DAG (both from database and network **Big work**)
* create a unit
* validate a unit
* save a unit
* broadcast a unit
* receive a unit
* stable a unit (commits unit)

## Scenario
the node act as a HUB, receive unit from headless wallet, validate and save it, and then broadcast to a normal JS version Hub and verify it works.

How to see that it works? By using the Trustnote explorer to verify if the unit is successfully saved on the main chain. 

## Challenges
* not fully understand every aspect of the Trustnote
* lack of qualified rust developers
* hard to absorb current JS implementation
* need Trustnote experts to participant in the project, from discussion to implementation and testing


## Time estimation of the project (total 25~35MD)
* project overall design 2 MD
* component break and interface design 5 MD
* component implementation - 10~20 MD
* unit test and integration test - 3 MD
* debug and fix errors need unexpected time - 5+ MD