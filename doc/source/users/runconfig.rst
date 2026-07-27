.. _run-config:

=====================
The run configuration
=====================

This page covers the CESM/NorESM run configuration and the attributes the
mediator reads from it. For what a run configuration *is* and how it relates to
the driver and the mediator, see :ref:`concepts`.

For CESM/NorESM the run configuration is the ``nuopc.runconfig`` file, which the
driver ingests at start-up. It is an ESMF-style configuration that defines the
active components, the processor layout, global driver attributes, and
per-component attributes; it also carries the :ref:`run sequence <run-sequence>`
block. As explained in the concepts, the mediator does not read this file
directly — the driver ingests it and *sets attributes* on the components, and the
mediator queries the attributes it needs.

Attributes the mediator reads
=============================

During its initialization phases the mediator retrieves attributes with
``NUOPC_CompAttributeGet``. Each lookup reports whether the attribute is present
and set, so optional attributes can fall back to defaults. The attributes CMEPS
consumes fall into a few groups:

* **Coupling mode** — ``coupling_mode`` selects which host exchange logic
  (``esmFldsExchange_<host>_mod.F90`` and ``fd_<host>.yaml``) and which custom
  ``prep`` calculations are used.
* **Atmosphere/ocean flux options** — for example ``aoflux_grid`` (the grid on
  which the air-sea fluxes are computed) and ``aoflux_code``.
* **Diagnostics and budgets** — for example ``do_budgets`` (enable the water and
  energy budget diagnostics).
* **Profiling and debugging** — for example ``Profiling`` and ``dbug_flag``.
* **Scalar-field descriptors** — the ``ScalarField*`` family (such as
  ``ScalarFieldName`` and ``ScalarFieldCount``) that describe the scalar values
  exchanged between the mediator and components.
* **Run control** — for example ``read_restart`` and the ``stop_*`` settings the
  mediator uses when setting its clock.

Most of these attributes are set for you by the compset and case configuration.
Where you can change them yourself, it is through ``user_nl_cpl`` or
``xmlchange`` as described in :ref:`running-a-case`.

.. note::

   The complete list of attributes CMEPS requires or recognizes — including
   which are required, which are optional, and which are specific to a
   particular host — is given in the :ref:`Reference <reference>`. This page
   orients you to the attributes the mediator reads; the Reference is the
   authoritative catalogue.
