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
**Результат** python оркестратор. Также создались недостающие инструкции для агентов. Запустил в 3 окнах терминала агентов с разными инструкциями. Запуск по типу go run agent_module/main.go -config configs/income-analyzer-config.md. Оркестратор успешно справился ✓ Application APP002 processing completed

Final Result:
{
  "applicant_id": "APP002",
  "stages": {
    "Data Collection": {
      "result": {
        "action": "Gather applicant information and documents",
        "input": {
          "annual_income": 120000,
          "applicant_id": "APP002",
          "documents": [
            "tax_return",
            "bank_statements"
          ],
          "employment_status": "self-employed",
          "name": "Jane Smith"
        },
        "status": "processed"
      },
      "trace_id": "trace-d6bf2888"
    },
    "Income Analysis": {
      "result": null,
      "error": "no matching rule for type: income_analysis.process",
      "trace_id": "trace-792bc60a"
    },
    "Risk Evaluation": {
      "result": null,
      "error": "no matching rule for type: risk_evaluation.process",
      "trace_id": "trace-864250f2"
    }
  },
  "status": "completed"
}
### Итого
- Количество промптов: 3
- Что пришлось исправлять вручную: создал make file для более удобного билда и теста работы.
- Время: ~ 30 минут
---
## Задание Повышенной сложности 3: Распределённая трассировка (Jaeger)
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** "Now integrate OpenTelemetry into agents and orchestrator. also setup Trace collection in Jaeger, which need to be launched in docker. Visualize full task completeion path"
**Результат:** файлы для докера, интеграция в агенты и оркестратор. Работа как ожидалось. Jaeger interface on http://localhost:16686. Все агенты видны, все отслеживается
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: ничего
- Время: ~ 10 минут.
---
## Задание Повышенной сложности 4:Агент с состоянием (Redis)
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** Create an agent who will save its condition into Redis (counters, stats, cache) and on reboot it will restore itsels. Can create a new md instruction file just for this client
**Результат:** Новый файл с инструкциями для агента котоырй сохраняет свое состояние. Запуск redis через docker-compose. Запуск запоминающего агента через ./agent -config configs/stateful-agent-config.md, посыл данных через ./test_client -subject stateful.analytics -type task. После force stop и перезапуска агента состояние сохраняется.
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: ничего
- Время: ~ 15 минут
---
## Задание Повышенной сложности 5:Динамическое масштабирование
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** "Create a system which will automatically launch another agent instance under high load (queue > N) use docker API. Container name - scaler."
**Результат:** Scaler в docker-compose.yml запущен и работает. Запуск скейлера через ./scaler -nats nats://localhost:4222 -scale-threshold 5 -scale-down-threshold 2 -min-instances 1 -max-instances 5 -check-interval 10s -agent-image agent:latest. Автоматически запускает по 1 экземпляру агента. При уменьшении нагрузки убирает экземпляры. Можно увидеть их с помощью docker ps --filter "label=managed-by=scaler"
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: ничего
- Время: ~ 20
---
## Задание Повышенной сложности 6:Аукционное распределение задач
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** "Now create a model where agents are auctioning for the task. Each agent rates its ability and orchestrator schooses agent with least cost or best compatability"
**Результат:**  Улучшенная модель аукциона с учетом стоимости и совместимости. Оркестратор сам выбирает лучшего агента.
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: проблемы с перезапуском docker, работой терминалов.
- Время: ~ 20 минут
---
## Задание Повышенной сложности 7:Интеграция LLM-агента
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** Use local Ollama with Qwen3.5:latest model to process messages. Implement a Risk Analysis system. Local model takes text and decides if there are risks - HIGH - MEDIUM - LOW. Orchestrator should block it if risk is HIGH, work if MEDIUM or LOW.
**Результат:** Запуск сервера через ollama serve. Модель работает и принимат запросы, но не всегда корректно определяет риски - проблема самой модели, промта и доступных мощностей.
### Промпт 2
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: Настройка простого Qwen3.5:latest через Ollama, фигурная скобка в risk_analysis.py, точное название модели в risk_analysis.py.
- Время: ~ 40 минут
---
## Задание Повышенной сложности 8:Веб-интерфейс для мониторинга агентов
### Промпт 1
**Инструмент:** Claude Haiku 4.5 в Agent режиме.
**Промпт:** "Finilize the app by adding a simplest web panel interface on fastapi+jinja2 for displaying agent status,queus, task results and task launch"
**Результат:** Базовая веб-панель с отображением статуса агентов, очередей, результатов задач и возможностью запуска задач. Доступна на http://localhost:8000
### Итого
- Количество промптов: 1
- Что пришлось исправлять вручную: ничего
- Время: ~ 20 минут
---