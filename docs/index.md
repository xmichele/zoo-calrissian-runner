# Introduction

[ZOO Calrissian Runner](http://www.zoo-project.org/) provides a bridge between the ZOO Project and Calrissian using [pycalrissian](https://github.com/Terradue/pycalrissian/).

The goal is to ease the development of runners for the EOEPCA ADES Zoo implementation.

A runner provides an execution engine for Zoo.

Below an overview of the building block

![Alt text](images/ades-overview.png "ADES Overview")

When a service is deployed, the ADES instantiates a cookiecutter processing service template.

EOEPCA provides:

* a example of a Zoo service template in the [https://github.com/EOEPCA/proc-service-template](https://github.com/EOEPCA/proc-service-template) software repository
* an implementation including the interaction with the Workspace API and the Catalog in the [https://github.com/EOEPCA/eoepca-proc-service-template](https://github.com/EOEPCA/eoepca-proc-service-template) software repository

Other service template can of course be implemented with different business logics and interfacing with other systems or APIs.
