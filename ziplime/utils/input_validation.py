# Copyright 2015 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from functools import partial
from operator import attrgetter

from numpy import dtype
from toolz import valmap, complement
import toolz.curried.operator as op

from ziplime.utils.compat import wraps
from ziplime.utils.preprocess import call, preprocess

_qualified_name = attrgetter("__qualname__")


def ensure_dtype(func, argname, arg):
    """
    Argument preprocessor that converts the input into a numpy dtype.

    Examples
    --------
    >>> import numpy as np
    >>> from ziplime.utils.preprocess import preprocess
    >>> @preprocess(dtype=ensure_dtype)
    ... def foo(dtype):
    ...     return dtype
    ...
    >>> foo(float)
    dtype('float64')
    """
    try:
        return dtype(arg)
    except TypeError as exc:
        raise TypeError(
            "{func}() couldn't convert argument "
            "{argname}={arg!r} to a numpy dtype.".format(
                func=_qualified_name(func),
                argname=argname,
                arg=arg,
            ),
        ) from exc


def expect_dtypes(__funcname=_qualified_name, **named):
    """
    Preprocessing decorator that verifies inputs have expected numpy dtypes.

    Examples
    --------
    >>> from numpy import dtype, arange, int8, float64
    >>> @expect_dtypes(x=dtype(int8))
    ... def foo(x, y):
    ...    return x, y
    ...
    >>> foo(arange(3, dtype=int8), 'foo')
    (array([0, 1, 2], dtype=int8), 'foo')
    >>> foo(arange(3, dtype=float64), 'foo')  # doctest: +NORMALIZE_WHITESPACE
    ...                                       # doctest: +ELLIPSIS
    Traceback (most recent call last):
       ...
    TypeError: ...foo() expected a value with dtype 'int8' for argument 'x',
    but got 'float64' instead.
    """
    for name, type_ in named.items():
        if not isinstance(type_, (dtype, tuple)):
            raise TypeError(
                "expect_dtypes() expected a numpy dtype or tuple of dtypes"
                " for argument {name!r}, but got {dtype} instead.".format(
                    name=name,
                    dtype=dtype,
                )
            )

    if isinstance(__funcname, str):

        def get_funcname(_):
            return __funcname

    else:
        get_funcname = __funcname

    @preprocess(dtypes=call(lambda x: x if isinstance(x, tuple) else (x,)))
    def _expect_dtype(dtypes):
        """
        Factory for dtype-checking functions that work with the @preprocess
        decorator.
        """

        def error_message(func, argname, value):
            # If the bad value has a dtype, but it's wrong, show the dtype
            # name.  Otherwise just show the value.
            try:
                value_to_show = value.dtype.name
            except AttributeError:
                value_to_show = value
            return (
                "{funcname}() expected a value with dtype {dtype_str} "
                "for argument {argname!r}, but got {value!r} instead."
            ).format(
                funcname=get_funcname(func),
                dtype_str=" or ".join(repr(d.name) for d in dtypes),
                argname=argname,
                value=value_to_show,
            )

        def _actual_preprocessor(func, argname, argvalue):
            if getattr(argvalue, "dtype", object()) not in dtypes:
                raise TypeError(error_message(func, argname, argvalue))
            return argvalue

        return _actual_preprocessor

    return preprocess(**valmap(_expect_dtype, named))


def make_check(exc_type, template, pred, actual, funcname):
    """
    Factory for making preprocessing functions that check a predicate on the
    input value.

    Parameters
    ----------
    exc_type : Exception
        The exception type to raise if the predicate fails.
    template : str
        A template string to use to create error messages.
        Should have %-style named template parameters for 'funcname',
        'argname', and 'actual'.
    pred : function[object -> bool]
        A function to call on the argument being preprocessed.  If the
        predicate returns `True`, we raise an instance of `exc_type`.
    actual : function[object -> object]
        A function to call on bad values to produce the value to display in the
        error message.
    funcname : str or callable
        Name to use in error messages, or function to call on decorated
        functions to produce a name.  Passing an explicit name is useful when
        creating checks for __init__ or __new__ methods when you want the error
        to refer to the class name instead of the method name.
    """
    if isinstance(funcname, str):

        def get_funcname(_):
            return funcname

    else:
        get_funcname = funcname

    def _check(func, argname, argvalue):
        if pred(argvalue):
            raise exc_type(
                template
                % {
                    "funcname": get_funcname(func),
                    "argname": argname,
                    "actual": actual(argvalue),
                },
            )
        return argvalue

    return _check


def expect_element(__funcname=_qualified_name, **named):
    """
    Preprocessing decorator that verifies inputs are elements of some
    expected collection.

    Examples
    --------
    >>> @expect_element(x=('a', 'b'))
    ... def foo(x):
    ...    return x.upper()
    ...
    >>> foo('a')
    'A'
    >>> foo('b')
    'B'
    >>> foo('c')  # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    Traceback (most recent call last):
       ...
    ValueError: ...foo() expected a value in ('a', 'b') for argument 'x',
    but got 'c' instead.

    Notes
    -----
    A special argument, __funcname, can be provided as a string to override the
    function name shown in error messages.  This is most often used on __init__
    or __new__ methods to make errors refer to the class name instead of the
    function name.

    This uses the `in` operator (__contains__) to make the containment check.
    This allows us to use any custom container as long as the object supports
    the container protocol.
    """

    def _expect_element(collection):
        if isinstance(collection, (set, frozenset)):
            # Special case the error message for set and frozen set to make it
            # less verbose.
            collection_for_error_message = tuple(sorted(collection))
        else:
            collection_for_error_message = collection

        template = (
            "%(funcname)s() expected a value in {collection} "
            "for argument '%(argname)s', but got %(actual)s instead."
        ).format(collection=collection_for_error_message)
        return make_check(
            ValueError,
            template,
            complement(op.contains(collection)),
            repr,
            funcname=__funcname,
        )

    return preprocess(**valmap(_expect_element, named))


def expect_bounded(__funcname=_qualified_name, **named):
    """
    Preprocessing decorator verifying that inputs fall INCLUSIVELY between
    bounds.

    Bounds should be passed as a pair of ``(min_value, max_value)``.

    ``None`` may be passed as ``min_value`` or ``max_value`` to signify that
    the input is only bounded above or below.

    Examples
    --------
    >>> @expect_bounded(x=(1, 5))
    ... def foo(x):
    ...    return x + 1
    ...
    >>> foo(1)
    2
    >>> foo(5)
    6
    >>> foo(6)  # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    Traceback (most recent call last):
       ...
    ValueError: ...foo() expected a value inclusively between 1 and 5 for
    argument 'x', but got 6 instead.

    >>> @expect_bounded(x=(2, None))
    ... def foo(x):
    ...    return x
    ...
    >>> foo(100000)
    100000
    >>> foo(1)  # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    Traceback (most recent call last):
       ...
    ValueError: ...foo() expected a value greater than or equal to 2 for
    argument 'x', but got 1 instead.

    >>> @expect_bounded(x=(None, 5))
    ... def foo(x):
    ...    return x
    ...
    >>> foo(6)  # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    Traceback (most recent call last):
       ...
    ValueError: ...foo() expected a value less than or equal to 5 for
    argument 'x', but got 6 instead.
    """

    def _make_bounded_check(bounds):
        (lower, upper) = bounds
        if lower is None:

            def should_fail(value):
                return value > upper

            predicate_descr = "less than or equal to " + str(upper)
        elif upper is None:

            def should_fail(value):
                return value < lower

            predicate_descr = "greater than or equal to " + str(lower)
        else:

            def should_fail(value):
                return not (lower <= value <= upper)

            predicate_descr = "inclusively between %s and %s" % bounds

        template = (
            "%(funcname)s() expected a value {predicate}"
            " for argument '%(argname)s', but got %(actual)s instead."
        ).format(predicate=predicate_descr)

        return make_check(
            exc_type=ValueError,
            template=template,
            pred=should_fail,
            actual=repr,
            funcname=__funcname,
        )

    return _expect_bounded(_make_bounded_check, __funcname=__funcname, **named)


def expect_strictly_bounded(__funcname=_qualified_name, **named):
    """
    Preprocessing decorator verifying that inputs fall EXCLUSIVELY between
    bounds.

    Bounds should be passed as a pair of ``(min_value, max_value)``.

    ``None`` may be passed as ``min_value`` or ``max_value`` to signify that
    the input is only bounded above or below.

    Examples
    --------
    >>> @expect_strictly_bounded(x=(1, 5))
    ... def foo(x):
    ...    return x + 1
    ...
    >>> foo(2)
    3
    >>> foo(4)
    5
    >>> foo(5)  # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    Traceback (most recent call last):
       ...
    ValueError: ...foo() expected a value exclusively between 1 and 5 for
    argument 'x', but got 5 instead.

    >>> @expect_strictly_bounded(x=(2, None))
    ... def foo(x):
    ...    return x
    ...
    >>> foo(100000)
    100000
    >>> foo(2)  # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    Traceback (most recent call last):
       ...
    ValueError: ...foo() expected a value strictly greater than 2 for
    argument 'x', but got 2 instead.

    >>> @expect_strictly_bounded(x=(None, 5))
    ... def foo(x):
    ...    return x
    ...
    >>> foo(5)  # doctest: +NORMALIZE_WHITESPACE +ELLIPSIS
    Traceback (most recent call last):
       ...
    ValueError: ...foo() expected a value strictly less than 5 for
    argument 'x', but got 5 instead.
    """

    def _make_bounded_check(bounds):
        (lower, upper) = bounds
        if lower is None:

            def should_fail(value):
                return value >= upper

            predicate_descr = "strictly less than " + str(upper)
        elif upper is None:

            def should_fail(value):
                return value <= lower

            predicate_descr = "strictly greater than " + str(lower)
        else:

            def should_fail(value):
                return not (lower < value < upper)

            predicate_descr = "exclusively between %s and %s" % bounds

        template = (
            "%(funcname)s() expected a value {predicate}"
            " for argument '%(argname)s', but got %(actual)s instead."
        ).format(predicate=predicate_descr)

        return make_check(
            exc_type=ValueError,
            template=template,
            pred=should_fail,
            actual=repr,
            funcname=__funcname,
        )

    return _expect_bounded(_make_bounded_check, __funcname=__funcname, **named)


def _expect_bounded(make_bounded_check, __funcname, **named):
    def valid_bounds(t):
        return isinstance(t, tuple) and len(t) == 2 and t != (None, None)

    for name, bounds in named.items():
        if not valid_bounds(bounds):
            raise TypeError(
                "expect_bounded() expected a tuple of bounds for"
                " argument '{name}', but got {bounds} instead.".format(
                    name=name,
                    bounds=bounds,
                )
            )

    return preprocess(**valmap(make_bounded_check, named))


def coerce(from_, to, **to_kwargs):
    """
    A preprocessing decorator that coerces inputs of a given type by passing
    them to a callable.

    Parameters
    ----------
    from : type or tuple or types
        Inputs types on which to call ``to``.
    to : function
        Coercion function to call on inputs.
    **to_kwargs
        Additional keywords to forward to every call to ``to``.

    Examples
    --------
    # >>> @preprocess(x=coerce(float, int), y=coerce(float, int))
    # ... def floordiff(x, y):
    # ...     return x - y
    # ...
    # >>> floordiff(3.2, 2.5)
    # 1
    #
    # >>> @preprocess(x=coerce(str, int, base=2), y=coerce(str, int, base=2))
    # ... def add_binary_strings(x, y):
    # ...     return bin(x + y)[2:]
    # ...
    # >>> add_binary_strings('101', '001')
    '110'
    """

    def preprocessor(func, argname, arg):
        if isinstance(arg, from_):
            return to(arg, **to_kwargs)
        return arg

    return preprocessor


coerce_string = partial(coerce, str)

