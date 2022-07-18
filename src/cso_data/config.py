from pathlib import Path

import git

repo = git.Repo('.', search_parent_directories=True)
REPO_ROOT = Path(repo.working_tree_dir)
DATA_ROOT = REPO_ROOT / "data"
METADATA_ROOT = DATA_ROOT / "metadata"
