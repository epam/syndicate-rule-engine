def get_last_element(string: str, delimiter: str) -> str:
    return string.split(delimiter)[-1]


severity_chain = {
    v: i for i, v in enumerate(('Info', 'Low', 'Medium', 'High'))
}


def severity_cmp(one: str, two: str) -> int:
    oi = severity_chain.get(one)
    ti = severity_chain.get(two)
    if not isinstance(oi, int):
        return 1
    if not isinstance(ti, int):
        return -1
    return oi - ti
