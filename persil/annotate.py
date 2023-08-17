from typing import Annotated, Any

from ptera.selector import Element
from ptera.tags import tag
from ptera.transform import transform
from typing_extensions import TypeVar

from .state import State

T = TypeVar("T", default=Any)

Persist = Annotated[T, tag.Persist]


class initialize:
    def __init__(self, value):
        self.value = value


class Interactor:
    def __init__(self, f, state):
        self.state = state

    def interact(self, symbol, key, category, value, overridable):
        if isinstance(value, initialize):
            return self.state.load_or_init(symbol, value.value)
        else:
            self.state[symbol] = value
            return value

    def __enter__(self):
        return self

    def __exit__(self, exctype, exc, tb):
        return


class AnnotatedState:
    def __init__(self, tag):
        self.tag = tag
        self.state = State()

    def decorate(self, fn):
        to_instrument = [Element(name=None, capture="it", category=self.tag)]
        tr = transform(
            fn,
            proceed=lambda f: Interactor(f, self.state),
            to_instrument=to_instrument,
            set_conformer=False,
            persist_annotations=True,
        )
        return tr

    def save(self):
        return self.state.save()


persist = AnnotatedState(tag=Persist)
