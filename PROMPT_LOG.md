## Задание Повышенной сложности 1: Разработка полной системы из 3–5 агентов на Go
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** "Write a single-purpose Go application that acts as a universal agent for a Credit Scoring system. It should read a Markdown configuration file on startup to determine its Role, Rules, and NATS Specialization (the queue it listens to). Keep the code lightweight. Provide the Go code and an example Markdown config for an 'Income Analyzer' agent."
**Результат:** Агент и md файл с инструкциями. Запускается как и ожидалосью. После дебага все успешно запустилось - 2026/05/22 18:25:45 [Income Analyzer] Subscribed successfully. Waiting for messages..
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: Пришлось вручную запускать локальный NATS сервер в docker, на стандартном порту 4222 (docker run -p 4222:4222 nats:latest). Также починил очередь в NATS (не принимала точки)
- Время: ~ 15 минут
---
## Задание Повышенной сложности 2:Цепочки задач (pipeline)
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:**  "now update the agent to connect to a NATS server. Implement an 'auction' mechanism: when the agent receives a task on a broadcast subject (like auction.risk_evaluation), it should calculate a mock cost based on its current queue length, and reply to the orchestrator with its bid. Provide the updated Go code."
**Результат:** Агент ожидает запроса. Однако пока ничего не поступает. Было принято решение создать тестового клиента на go который будет подавать задания и запросы на аукционы.
### Промпт 2
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** "Write a simple test client for sending tasks and auction requests to the agent"
**Результат** test_client.go файл. Отправляет запросы агенту. При запросе go run test_client.go -subject auction.income_eval -type auction агент отвечает 2026/05/22 18:33:34 [Income Analyzer] Received auction for income_eval (trace: trace-1779464014)
2026/05/22 18:33:34 [Income Analyzer] Submitted bid: cost=1.00, load=0/100 (trace: trace-1779464014)
### Промпт 3
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** "write an asyncio Python orchestrator using nats-py. It needs to manage a Credit Scoring pipeline: Data Collection -> Income Analysis -> Risk Evaluation. Add needed agent instructions into configs folder. The orchestrator must broadcast tasks, wait a few milliseconds to collect bids from the Go agents, assign the task to the lowest bidder, and handle timeouts (retry up to 3 times). Provide the Python code."
**Результат** 
### Итого
- Количество промптов: 3
- Что пришлось исправлять вручную: создал make file для более удобного билда и теста работы.
- Время: ~ 30 минут
---
## Задание Повышенной сложности 3: Распределённая трассировка (Jaeger)
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** 
**Результат:** 
### Промпт 2
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: 
- Время: ~
---
## Задание Повышенной сложности 4:Агент с состоянием (Redis)
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** 
**Результат:** 
### Промпт 2
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: 
- Время: ~
---
## Задание Повышенной сложности 5:Динамическое масштабирование
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** 
**Результат:** 
### Промпт 2
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: 
- Время: ~
---
## Задание Повышенной сложности 6:Аукционное распределение задач
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** 
**Результат:** 
### Промпт 2
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: 
- Время: ~
---
## Задание Повышенной сложности 7:Интеграция LLM-агента
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** 
**Результат:** 
### Промпт 2
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: []
- Время: ~
---
## Задание Повышенной сложности 8:Веб-интерфейс для мониторинга агентов
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** 
**Результат:** 
### Промпт 2
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: 
- Время: ~
---