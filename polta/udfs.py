from json import load as json_load


def file_path_to_payload(file_path: str) -> str:
  return open(file_path, 'r').read()

def file_path_to_json(file_path: str) -> str:
  return json_load(open(file_path, 'r'))
