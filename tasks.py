from invoke import task


@task(name="lint")
def lint(c):
    paths_to_lint = ["src/etl_microservice", "tasks.py"]

    c.run(f"isort {' '.join(paths_to_lint)}", pty=True)

    c.run(f"ruff {' '.join(paths_to_lint)}", pty=True)

    c.run(f"mypy {' '.join(paths_to_lint)}", pty=True)

    c.run(f"black --check {' '.join(paths_to_lint)}", pty=True)
