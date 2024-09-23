import typer


def cmd(name: str):
    print(f"Hello {name}")


def entrypoint():
    typer.run(cmd)
