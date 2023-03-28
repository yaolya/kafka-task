# kafka-task
It is a flight booking application, which allows customers to book tickets for airline flights.   
An airline announces a flight and sends a message to a flight topic. Customer and flight services are subscribed to that topic and receive the message. Then a reservation can be made by a customer and published to the booking topic. From there it is read by the flight service and if there are available seats and reservation details are valid a reservation is approved. The response goes to the response topic and is received by a customer. Flight information can be updated by the airline and changes received by customer and flight services.  

## requirements
requirements.txt

## list of commands to launch and test

`docker-compose up`

`python3 airline/main.py`  
`python3 customer/main.py`  
`python3 flight/main.py`  

`http://127.0.0.1:8000/docs` - airline service  
`http://127.0.0.1:8001/docs` - flight service  
`http://127.0.0.1:8002/docs` - customer service   

## application design diagram
diagram.png
