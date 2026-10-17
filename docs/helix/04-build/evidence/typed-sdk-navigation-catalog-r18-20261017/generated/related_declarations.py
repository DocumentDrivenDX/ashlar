from umf_access import Param, query
from native_related.models import Order
from native_related.keys import OrderKey

@query
def related_rows():
    return Order.objects.select(Order.c.id).include_keys(Order.c.customers, limit=2).order_by(Order.c.id.asc()).limit(10).all()

@query
def related_pages(after: Param[OrderKey | None]):
    return Order.objects.select(Order.c.id).include_keys(Order.c.customers, limit=2).page(size=1, after=after)

from native_related.models import Customer
from native_related.keys import CustomerKey

@query
def navigation_uncapped(source_status: Param[str], target_name: Param[str]):
    return Order.objects.filter(Order.c.status == source_status).related(Order.c.customers).filter(Customer.c.customer_name == target_name).select(Customer.c.customer_id).order_by(Customer.c.customer_id.asc()).limit(10).all()

@query
def navigation_limited():
    return Order.objects.order_by(Order.c.id.asc()).limit(1).related(Order.c.customers).select(Customer.c.customer_id).order_by(Customer.c.customer_id.asc()).limit(10).all()

@query
def navigation_inverse():
    return Customer.objects.filter(Customer.c.customer_id == 'customer-a').related(Order.c.customers.inverse()).select(Order.c.id).order_by(Order.c.id.asc()).limit(10).all()

@query
def navigation_pages(after: Param[CustomerKey | None]):
    return Order.objects.filter(Order.c.status == 'draft').related(Order.c.customers).select(Customer.c.customer_id).page(size=1, after=after)
