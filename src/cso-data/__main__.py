from pathlib import Path
import yaml
import click

from box import Box


# Inspired by https://medium.com/bcggamma/welcome-to-the-big-leagues-b9038648054f
# and also by https://medium.com/bcggamma/data-science-python-best-practices-fdb16fdedf82

# *Config file(s) should live in the project repo, but outside of the src/ structure.


@click.command()
@click.option("-cfg", "--config_path", default="./config.yml")
def run(config_path):
    # Read in yaml file
    with open(Path(config_path), "r") as ymlfile:  
        # Use Box to allow dot notation for config
        config = Box(
            yaml.safe_load(ymlfile)
        )  
    print(
        f"Hello {config.project_name}!"
    )  # Check config is working using friendly message!


if __name__ == "__main__":
    run()
