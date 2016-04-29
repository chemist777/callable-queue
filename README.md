# callable-queue

Очередь заданий (CallableQueue) хранится в памяти сервера. Метод добавления заданий(submit) может вызываться из нескольких потоков.

Параметром конструктора backlog задаётся максимальный размер очереди, при превышении которого задания выполняются досрочно.

Параметр конструктора workerThreads задаёт число потоков, выполняющих задания.

Если не хватает памяти на одном сервере, то программа запускается на нескольких. При этом задания клиент добавляет на произвольный сервер.

Время на серверах должно быть синхронизированно.


Для работы необходима Java 8, для сборки maven.


Прилагаются тесты, для проверки.