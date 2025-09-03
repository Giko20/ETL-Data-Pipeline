{% macro price_change_ndays(n=7, column="price", partition_by="asset_id", order_by="date") %}
    {{ return(
        "({column} - lag({column}, {n}) over (partition by {partition_by} order by {order_by})) as price_change_{n}d".format(
            column=column,
            n=n,
            partition_by=partition_by,
            order_by=order_by
        )
    ) }}
{% endmacro %}