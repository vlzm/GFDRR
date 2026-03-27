{{ fullname | escape | underline }}

.. automodule:: {{ fullname }}

{% if classes %}
Classes
-------

.. autosummary::
   :toctree:
   :nosignatures:

{% for item in classes %}
   {{ item }}
{% endfor %}
{% endif %}

{% if functions %}
Functions
---------

.. autosummary::
   :toctree:
   :nosignatures:

{% for item in functions %}
   {{ item }}
{% endfor %}
{% endif %}

{% if modules %}
Submodules
----------

.. autosummary::
   :toctree:
   :recursive:

{% for item in modules %}
   {{ item }}
{% endfor %}
{% endif %}
