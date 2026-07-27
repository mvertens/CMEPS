.. _index:

###################
CMEPS documentation
###################

The Community Mediator for Earth Prediction Systems (CMEPS) is a
NUOPC-compliant mediator used to couple the components of an Earth
system model. It is used by, among others, NCAR's Community Earth System
Model (CESM), NorESM, and NOAA's UFS and HAFS coupled systems.

This documentation is organized into four parts:

* **Overview & Concepts** introduces what the mediator does and the ideas
  the rest of the documentation relies on. Start here.
* **User Guide** is for people configuring and running a coupled system
  that uses CMEPS.
* **Developer Guide** is for people modifying the mediator: its code
  structure, coupling phases, and how fields are exchanged, mapped and merged.
* **Reference** collects field-name and attribute tables and a glossary.

.. toctree::
   :maxdepth: 2
   :numbered:
   :caption: Overview & Concepts

   overview/index

.. toctree::
   :maxdepth: 2
   :numbered:
   :caption: User Guide

   users/index

.. toctree::
   :maxdepth: 2
   :numbered:
   :caption: Developer Guide

   developer/index

.. toctree::
   :maxdepth: 2
   :numbered:
   :caption: Reference

   reference/index

.. Legacy pages retained (hidden) while their content is migrated into the
   four parts above. Remove entries here as each page is retired.
.. toctree::
   :hidden:
   :caption: Legacy (being migrated)

   introduction
   esmflds
   fractions
   generic
   prep
   addendum/index
