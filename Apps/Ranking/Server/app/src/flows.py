from urllib.parse import parse_qs


def path_to_flow(path):
    flow = []

    for segment in path.split('/'):
        if segment:
            if ';' in segment:
                segment, params = segment.split(';', 1)
                params = parse_qs(params)
                for key, value in params.items():
                    if len(value) == 1:
                        params[key] = value[0]
            else:
                params = {}
            flow.append({
                "algorithm": segment,
                "params": params
            })

    return {'flow': flow}


schema = {
    "type": "object",
    "properties": {
        "flow": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "algorithm": {"type": "string"},
                    "params": {
                        "type": "object",
                    },
                }
            }
        },
    },
}
