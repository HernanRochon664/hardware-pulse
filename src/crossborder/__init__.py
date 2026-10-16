"""
"¿Comprar acá o importar?": landed cost of buying in the US and shipping to
Uruguay, compared with the best local price.

- ``config``     typed loaders for the rules, couriers, US stores and weight profiles
- ``couriers``   courier charges (chargeable weight, freight, TFSPU, handling...)
- ``engine``     one shipment, line by line, under the franchise or the 60% regime
- ``verdict``    "conviene importar o no", alerts and the risks kept apart from the total
- ``optimizer``  several products: what to buy here, what to import and how to group it
- ``intake``     parsing what the bookmarklet sends and matching it to the catalog

Pure Python (no Streamlit) so every number can be unit tested.
"""
