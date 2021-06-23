# binding-corruption-test

## BindingCorruptionTest

Spring Boot App to recreate "binding corruption" where after a TAS recreate of a 3 node rabbitmq cluster, we see issues where a queue and binding exists but messages published to the routing key do not route to the queue.

This app is creating transient, auto-delete queues that are bound to a durable topic exchange.

We also have the following server-side policy applied to these queues:

![Queue Policy](policy.PNG)

## BindingCorruptionDetector

Finds all the routing keys belonging to the durable topic exchange and publishes a mandatory message on each one. The expectation is that no mandatory messages are returned. If they are, then this is what I am referring to as a "corrupt binding".

Re-declaring the binding does not fix it. You have to first unbind and then re-bind and then messages will start flowing to the queue again.

## Configuration:

To Run `BindingCorruptionTest`, activate the `test` spring profile or configure `--binding-corruption-test.enabled=true`

To Run `BindingCorruptionDetector`, activate the `detector` spring profile or configure `--binding-corruption-detector.enabled=true`

See the following classes for additional configuration:

1. [RabbitProperties](https://docs.spring.io/spring-boot/docs/current/api/org/springframework/boot/autoconfigure/amqp/RabbitProperties.html)
2. [BindingCorruptionTestProperties](src/main/java/com/cerner/test/BindingCorruptionTestProperties.java)
3. [BindingCorruptionDetectorProperties](src/main/java/com/cerner/test/BindingCorruptionDetectorProperties.java)

## Notes:

Recreating seems to be based on the load on the cluster and how fast the connections reconnect after connection loss.

On a cluster with 5k queues and no message traffic:
 - I can only recreate with a delay < 1 second

On a cluster with 5k queues and roughly 50-100 messages/sec of traffic:
 - delays less than < 1 second generally create 2k+ corrupt bindings
 - I've tested all the way up to a 10 second delay and still seeing hundreds of binding corruptions
