# Bulk 1.0 API

[back](../../README.md)

The `bulkv1` pacakge is a wrapper for the [Salesforce Bulk
API](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_intro.htm).
In most cases the [Bulk v2 API](../README.md) is more ergonomic, but it's missing support for
controlled concurrency mode and batch size.

Note that the Bulk v1 API requires a [Session
ID](https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_basics_session_header.htm)
(typically set as the `X-SFDC-Session` HTTP header) for authentication; ensure
your `ServiceFormatter` adds the appropriate header.
