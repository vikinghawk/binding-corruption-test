# binding-corruption-test

## BindingCorruptionTest

Spring Boot App to recreate "binding corruption" where after a TAS recreate of a 3 node rabbitmq cluster, we see issues where a queue and binding exists but you cannot publish messages to it.

This app is creating transient, auto-delete queues that are bound to a durable topic exchange.

We also have the following server-side policy applied to these queues:

![Queue Policy](policy.png)

## BindingCorruptionDetector

Finds all the routing keys belonging to the durable topic exchange and publishes a mandatory message on each one. The expectation is that no mandatory messages are returned. If they are, then this is what I am referring to as a "corrupt binding".

## Configuration:

To Run `BindingCorruptionTest`, activate the `test` spring profile or configure `--binding-corruption-test.enabled=true`

To Run `BindingCorruptionDetector`, activate the `detector` spring profile or configure `--binding-corruption-detector.enabled=true`

See the following classes for additional configuration:

1. [RabbitProperties](https://docs.spring.io/spring-boot/docs/current/api/org/springframework/boot/autoconfigure/amqp/RabbitProperties.html)
2. [BindingCorruptionTestProperties](src/main/java/com/cerner/test/BindingCorruptionTestProperties.java)
3. [BindingCorruptionDetectorProperties](src/main/java/com/cerner/test/BindingCorruptionDetectorProperties.java)