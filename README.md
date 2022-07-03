# homework-5

Реализовать батчевый Kafka-consumer, который считывает N сообщений из топика и отдаёт их на обработку делегату обработчику `void ProcessMessage<TKey, TValue>(IReadOnlyCollection<Message<TKey, TValue>> message)`, который принимает на вход массив сообщений из топика.

Важно предусмотреть:
- корректную обработку ошибок
- правильное сохранение оффсета в рамках партиции


Задание со звёздочкой:
реализовать exactly-once / идемпотентную обработку сообщений из топика с помощью редиса в качестве хранилища.

## Принцип работы

Запуск kafka в docker:

`docker run -p 2181:2181 -p 9092:9092 -e ADVERTISED_HOST=127.0.0.1  -e NUM_PARTITIONS=10 johnnypark/kafka-zookeeper`

Запуск redis в docker:

`docker run -p 6379:6379 redis`
