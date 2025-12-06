# -*- coding: utf-8 -*-

# execution.py

import datetime
from queue import Queue
import uuid

from src.engine.events import ExecutionEvent, OrderEvent

from ..base import ExecutionHandler


class SimulatedExecutionHandler(ExecutionHandler):
    """
    The simulated execution handler simply converts all order objects into
    their equivalent execution events automatically without latency, slippage
    or fill-ratio issues.

    This allows a straightforward "first go" test of any strategy, before
    implementation with a more sophisticated execution handler.
    """

    def __init__(self, events: Queue) -> None:
        """
        Initialises the handler, setting the event queues up internally.

        Parameters:
        events - The Queue of Event objects.
        """
        self.events: Queue = events

    def execute_order(self, event: OrderEvent) -> None:
        """
        Simply converts Order objects into ExecutionEvent objects naively,
        i.e. without any latency, slippage or fill ratio problems.

        Parameters:
        event - Contains an Event object with order information.
        """
        if event.type == 'ORDER':
            execution_event = ExecutionEvent(
                order_id=event.order_id if event.order_id else str(uuid.uuid4()),
                status='FILLED',
                symbol=event.symbol,
                direction=event.direction,
                filled_quantity=event.quantity,
                remaining_quantity=0,
                avg_fill_price=0.0,  # Simulated - would be actual price in live
                commission=0.0,
                timestamp=datetime.datetime.utcnow()
            )
            self.events.put(execution_event)
