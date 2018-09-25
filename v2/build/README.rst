Containerized Gazette Build
===========================

See ``all.sh`` for run-able documentation of repository containers
and build processes.

Publish The Base Runtime Image
==============================

Build a new base image::

  docker build . -f build/Dockerfile.gazette-base --tag gazette-base

Test it locally by temporarily changing the references in
``build/Dockerfile.gazette-build`` and ``build/cmd/Dockerfile.gazette`` from
``liveramp/gazette-base:X.Y.Z`` [*]_ to ``gazette-base``. Then run the ``docker
build`` commands given above.

Once the image has been tested and is ready to publish, pick an appropriate
`semantic version number`_ [*]_ then tag and push the image::

  docker tag gazette-base liveramp/gazette-base:latest
  docker tag gazette-base liveramp/gazette-base:X.Y.Z
  docker push liveramp/gazette-base:latest
  docker push liveramp/gazette-base:X.Y.Z

It may be necessary to log in to Docker Hub before ``docker push``::

  docker login  # Interactively enter username and password

.. _semantic version number: https://semver.org

.. [*] Note that this project's convention is to not prefix the version number
       with "v".
.. [*] For example, changing major versions of Go would be a major version
       bump, installing an additional tool would be a minor version bump, and
       fixing a bug in a ``RUN`` command would be a patch version bump.
