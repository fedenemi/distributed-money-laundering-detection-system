import logging
from paths_creator.paths_creator import PathsCreator

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    paths_creator = PathsCreator()
    paths_creator.run()