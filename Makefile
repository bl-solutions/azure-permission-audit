up: app_run
down: compose_down

app_run: compose_up
	@python main.py

compose_up:
	@docker compose up -d

compose_down:
	@docker compose down