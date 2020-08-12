# Limits Request API
[back](../README.md)

The `limits` package provides a `Composite Batch` sub-request implementation
that is able to fetch organization limit information.

As a reference, see the Salesforce [Limit Resource
documentation](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_limits.htm).

```go
resource, err := batch.NewResource(session)
if err != nil {
    fmt.Printf("Batch Composite Error %s\n", err.Error())
    return
}

value, err := resource.Retrieve(false, []batch.Subrequester{
    limits.NewSubrequester(),
})
if err != nil {
    fmt.Printf("Batch Composite Error %s\n", err.Error())
    return
}

// value.Results[0] will contain the limit request response;
// if successful, value.Results[0].Result will be a map[string]interface{}
// containing the limits as described in the documentation.
fmt.Printf("%+v\n", value)
```
