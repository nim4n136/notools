import re

def snack_case_list(list_col: list):
    return [snack_case(i) for i in list_col]

def snack_case(strings):
    strings = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', strings)
    strings = re.sub('([a-z0-9])([A-Z])', r'\1_\2', strings).lower()
    strings = re.sub('[^a-zA-Z0-9 \n\.]', '_', strings)
    strings = strings.replace(" ","_")
    return strings