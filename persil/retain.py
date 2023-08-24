from datetime import timedelta


class RetentionPolicy:
    def include_next(self, entry, history):  # pragma: no cover
        return False

    def cull(self, history):
        return set()

    def __and__(self, other):
        return IntersectionRetention(self, other)

    def __or__(self, other):
        return UnionRetention(self, other)


######################################
# Combinations of retention policies #
######################################


class CombinedRetention(RetentionPolicy):
    def __init__(self, a, b):
        self.a = a
        self.b = b


class IntersectionRetention(CombinedRetention):
    def include_next(self, entry, history):
        return self.a.include_next(entry, history) and self.b.include_next(
            entry, history
        )

    def cull(self, history):
        return self.a.cull(history) | self.b.cull(history)


class UnionRetention(CombinedRetention):
    def include_next(self, entry, history):
        return self.a.include_next(entry, history) or self.b.include_next(
            entry, history
        )

    def cull(self, history):
        return self.a.cull(history) & self.b.cull(history)


#############################
# Common retention policies #
#############################


class ThrottledRetention(RetentionPolicy):
    def __init__(self, seconds=0, minutes=0, hours=0, milliseconds=0):
        self.delta = timedelta(
            seconds=seconds, minutes=minutes, hours=hours, milliseconds=milliseconds
        )

    def include_next(self, entry, history):
        if not history:
            return True
        now = entry["timestamp"]
        last = history[-1]["timestamp"]
        if timedelta(seconds=now - last) >= self.delta:
            return True
        return False


class SerialRetention(RetentionPolicy):
    def __init__(self, numbers):
        self.numbers = numbers

    def include_next(self, entry, history):
        return entry["serial"] in self.numbers


class SerialModuloRetention(RetentionPolicy):
    def __init__(self, interval, offset=0):
        self.interval = interval
        self.offset = (self.interval - offset) % self.interval

    def include_next(self, entry, history):
        return ((entry["serial"] + self.offset) % self.interval) == 0


class ExtremumRetention(RetentionPolicy):
    def __init__(self, key, method=min):
        if isinstance(key, str):
            self.key = lambda x: x.get("data", {}).get(key, None)
        else:
            self.key = key
        self.method = method
        self._current_best = None

    def include_next(self, entry, history):
        candidate = self.key(entry)
        if candidate is None or self._current_best == candidate:
            return False
        elif (
            self._current_best is None
            or self.method(candidate, self._current_best) == candidate
        ):
            self._current_best = candidate
            return True
        else:
            return False

    def cull(self, history):
        return {
            entry["serial"]
            for entry in history
            if self.key(entry) != self._current_best
        }


class ConditionalRetention(RetentionPolicy):
    def __init__(self, conditions):
        self.conditions = {
            k: check if callable(check) else lambda x, check=check: x == check
            for k, check in conditions.items()
        }

    def include_next(self, entry, history):
        data = entry["data"]
        for key, check in self.conditions.items():
            value = data.get(key, None)
            if check(value):
                return True
        else:
            return False


class LimitedRetention(RetentionPolicy):
    def __init__(self, max_entries, basis="timestamp"):
        self.max_entries = max_entries
        self.basis = basis

    def desirability(self, history, index):
        t = history[index][self.basis]
        result = 0
        if index > 0:
            result += t - history[index - 1][self.basis]
        else:
            result += 100
        if index < len(history) - 1:
            result += history[index + 1][self.basis] - t
        else:
            result += 100
        return result

    def include_next(self, entry, history):
        return True

    def cull(self, history):
        history = list(history)
        ncut = len(history) - self.max_entries
        results = set()
        if ncut <= 0:
            return results
        while ncut > 0:
            _, index, serial = min(
                [
                    (self.desirability(history, i), i, entry["serial"])
                    for i, entry in enumerate(history)
                ]
            )
            del history[index]
            results.add(serial)
            ncut -= 1
        return results


def serial(*numbers):
    return SerialRetention(numbers)


def every(n, offset=0):
    return SerialModuloRetention(interval=n, offset=offset)


def throttle(seconds=0, **others):
    return ThrottledRetention(seconds=seconds, **others)


def minimum(key):
    return ExtremumRetention(key, method=min)


def maximum(key):
    return ExtremumRetention(key, method=max)


def at_most(max_entries):
    return LimitedRetention(max_entries=max_entries)


def whenever(**conditions):
    return ConditionalRetention(conditions)


######################
# Policy application #
######################


class RetentionApplicator:
    def __init__(self, policy, culler=None):
        self.policy = policy
        self.culler = culler

    def __call__(self, entry, history, calculate_history=True):
        if not self.policy.include_next(entry, history):
            return False, history
        new_history = []
        if calculate_history:
            history.append(entry)
            to_remove = self.policy.cull(history)
            for entry in history:
                if entry["serial"] in to_remove:
                    if self.culler is not None:
                        self.culler(entry)
                else:
                    new_history.append(entry)
        return True, new_history
