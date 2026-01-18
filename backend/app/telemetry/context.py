from opentelemetry import propagate


def inject_trace_context(carrier: dict):
    """
    Inject current trace context into a carrier dict.
    """
    propagate.inject(carrier)


def extract_trace_context(carrier: dict):
    """
    Extract trace context from a carrier dict.
    """
    return propagate.extract(carrier)
