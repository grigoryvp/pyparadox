=========
pyparadox
=========

Simple Paradox database reader

.. toctree::
  :hidden:

  Home <self>
  API <pyparadox>

Why?
====

I have a closed source third party application running on the server that
use Paradox database. In order to communicate with this application i read
Paradox database directly, but it takes about 10 minutes for a simple query
to execute due to the outdated database drivers. This tool reads
Paradox database binary file in background and updates SQLite database file
that mirrors changes. SQLite databse is than used to access data fast and
reliable way.
