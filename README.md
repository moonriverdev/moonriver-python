# Moon River Python

## Sending messages

~~~ python
moon = MoonRiver("localhost:5672")
greetings = Sender(moon, "greetings")

with moon.start():
    greetings.send("hello")
~~~

## Receiving messages

~~~ python
moon = MoonRiver("localhost:5672")

@moon.receiver("greetings")
def receive_greeting(delivery):
    print(delivery.message)
~~~
