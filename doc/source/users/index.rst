.. _users:

##########
User Guide
##########

This part is for people configuring and running a coupled system that uses the
CMEPS mediator.

A coupled run is driven by **two key inputs that the NUOPC driver ingests** at
start-up:

* the **run sequence** — the ordered recipe of component runs, mediator phases
  and field transfers, together with the coupling intervals (:ref:`run-sequence`); and
* the **run configuration** — the attributes the driver and the mediator read
  (:ref:`run-config`).

The pages below describe each of these two inputs in turn, and then show how a
CESM/NorESM case is configured to produce them.

.. important::

   **This User Guide is specific to CESM and NorESM.** Because the driver, the
   run-configuration file, and the case-control tooling are host-specific (see
   :ref:`run-sequence`), the operational guidance here — how a run is
   configured and driven — applies to the CESM/NorESM applications. Users of
   other applications such as **UFS** or **HAFS** should consult that
   application's own user documentation for the equivalent workflow.

   The mediator itself is shared across applications, so the :ref:`Overview
   <overview>` and :ref:`Developer Guide <developer>` are not host-specific —
   only this User Guide is.

.. note::

   This part is being written. More sections will be added here as the rewrite
   proceeds. See :ref:`overview` for the concepts these pages build on.

.. toctree::
   :maxdepth: 2

   run_sequence
   runconfig
   running
   fields
