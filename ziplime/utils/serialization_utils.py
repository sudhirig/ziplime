import pickle

CHECKSUM_KEY = '__state_checksum'


def load_context(state_file_path, context, checksum):
    return
    with open(state_file_path, 'rb') as f:
        try:
            loaded_state = pickle.load(f)
        except (pickle.UnpicklingError, IndexError):
            raise ValueError("Corrupt state file: {}".format(state_file_path))
        else:
            if CHECKSUM_KEY not in loaded_state or \
                    loaded_state[CHECKSUM_KEY] != checksum:
                raise TypeError("Checksum mismatch during state load. "
                                "The given state file was not created "
                                "for the algorithm in use")
            else:
                del loaded_state[CHECKSUM_KEY]

            for k, v in loaded_state.items():
                setattr(context, k, v)


def store_context(state_file_path, context, checksum, include_list):
    return
    state = {}

    for field in include_list:
        try:
            state[field] = getattr(context, field)
        except Exception as e:
            continue

    state[CHECKSUM_KEY] = checksum

    with open(state_file_path, 'wb') as f:
        pickle.dump(state, f, protocol=5)
