package soql

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	// escapeString defines a String Replacer which will correctly escape values
	// in quotes SOQL strings.
	escapeString = strings.NewReplacer(
		`'`, `\'`,
		`\`, `\\`,
	)
)

// QueryInput is used to provide SOQL inputs.
//
// # ObjectType is the Salesforce Object, like Account
//
// # FieldList is the Salesforce Object's fields to query
//
// # SubQuery is the inner query
//
// # Where is the SOQL where cause
//
// # Order is the SOQL ordering
//
// # Limit is the SOQL record limit
//
// Offset is the SOQL record offset
type QueryInput struct {
	FieldList  []string
	ObjectType string
	SubQuery   []QueryFormatter
	Where      WhereClauser
	Order      Orderer
	Limit      int
	Offset     int
}

// Query is the struture used to build a SOQL query.
type Query struct {
	fieldList  []string
	objectType string
	subQuery   []QueryFormatter
	where      WhereClauser
	order      Orderer
	limit      int
	offset     int
}

// QueryFormatter is the interface to return the SOQL query.
//
// Format returns the SOQL query.
type QueryFormatter interface {
	Format() (string, error)
}

// AggregationQueryFormatter is a QueryFormatter that takes a base formatter
// and produces a query string with an order by, limit, and offset in the query.
// It is to be used with aggregation queries (GROUP BY) where paging is not
// supported by Salesforce's SOQL API; so we manually page using limit offset.
type AggregationQueryFormatter struct {
	baseFormat QueryFormatter
	orderBy    string
	offset     int
	limit      int
}

func (a *AggregationQueryFormatter) Format() (string, error) {
	base, err := a.baseFormat.Format()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s ORDER BY %s LIMIT %d OFFSET %d", base, a.orderBy, a.limit, a.offset), nil
}

type QueryOffsetLimiter interface {
	QueryFormatter
	UseOffsetLimit() bool
}

// NewQuery creates a new builder.  If the object is an
// empty string, then an error is returned.
func NewQuery(input QueryInput) (*Query, error) {
	if input.ObjectType == "" {
		return nil, errors.New("builder: object type can not be an empty string")
	}
	if len(input.FieldList) == 0 {
		return nil, errors.New("builder: field list can not be empty")
	}

	return &Query{
		objectType: input.ObjectType,
		fieldList:  input.FieldList,
		subQuery:   input.SubQuery,
		where:      input.Where,
		order:      input.Order,
		limit:      input.Limit,
		offset:     input.Offset,
	}, nil
}

// Format will return the SOQL query.  If the builder has an empty string or
// the field list is zero, an error is returned.
func (b *Query) Format() (string, error) {
	if b.objectType == "" {
		return "", errors.New("builder: object type can not be an empty string")
	}
	if len(b.fieldList) == 0 {
		return "", errors.New("builder: field list must be have fields present")
	}

	soql := "SELECT " + strings.Join(b.fieldList, ",")
	if b.subQuery != nil {
		for _, query := range b.subQuery {
			var sub string
			var err error
			if sub, err = query.Format(); err == nil {
				soql += fmt.Sprintf(",(%s)", sub)
			} else {
				return "", err
			}
		}
	}
	soql += " FROM " + b.objectType
	if b.where != nil {
		soql += " " + b.where.Clause()
	}
	if b.order != nil {
		order, err := b.order.Order()
		if err == nil {
			soql += " " + order
		} else {
			return "", err
		}
	}
	if b.limit > 0 {
		soql += fmt.Sprintf(" LIMIT %d", b.limit)
	}
	if b.offset > 0 {
		soql += fmt.Sprintf(" OFFSET %d", b.offset)
	}
	return soql, nil
}

// WhereClause is the structure that will contain a SOQL where clause.
type WhereClause struct {
	expression string
}

// WhereExpression is an interface to return the where cause's expression.
type WhereExpression interface {
	Expression() string
}

// WhereClauser is an interface to return the where cause.
type WhereClauser interface {
	Clause() string
}

// WhereLike will form the LIKE expression.
func WhereLike(field string, value string) (*WhereClause, error) {
	if field == "" {
		return nil, errors.New("soql where: field can not be empty")
	}
	if value == "" {
		return nil, errors.New("soql where: value can not be empty")
	}
	return &WhereClause{
		expression: fmt.Sprintf("%s LIKE '%s'", field, value),
	}, nil
}

// WhereGreaterThan will form the greater or equal than expression.  If the value is a
// string or boolean, an error is returned.
func WhereGreaterThan(field string, value interface{}, equals bool) (*WhereClause, error) {
	if field == "" {
		return nil, errors.New("soql where: field can not be empty")
	}

	v, err := formatValue(value)
	if err != nil {
		return nil, fmt.Errorf("where greater than: %w", err)
	}

	operator := ">"
	if equals {
		operator += "="
	}

	return &WhereClause{
		expression: fmt.Sprintf("%s %s %s", field, operator, v),
	}, nil
}

// WhereLessThan will form the less or equal than expression.  If the value is a
// string or boolean, an error is returned.
func WhereLessThan(field string, value interface{}, equals bool) (*WhereClause, error) {
	if field == "" {
		return nil, errors.New("soql where: field can not be empty")
	}

	v, err := formatValue(value)
	if err != nil {
		return nil, fmt.Errorf("where less than: %w", err)
	}

	operator := "<"
	if equals {
		operator += "="
	}

	return &WhereClause{
		expression: fmt.Sprintf("%s %s %s", field, operator, v),
	}, nil
}

// WhereEquals forms the equals where expression.
func WhereEquals(field string, value interface{}) (*WhereClause, error) {
	if field == "" {
		return nil, errors.New("soql where: field can not be empty")
	}
	v, err := formatValue(value, AllowBool(), AllowNil())
	if err != nil {
		return nil, fmt.Errorf("where equal: %w", err)
	}

	return &WhereClause{
		expression: fmt.Sprintf("%s = %s", field, v),
	}, nil
}

// WhereNotEquals forms the not equals where expression.
func WhereNotEquals(field string, value interface{}) (*WhereClause, error) {
	if field == "" {
		return nil, errors.New("soql where: field can not be empty")
	}
	v, err := formatValue(value, AllowBool(), AllowNil())
	if err != nil {
		return nil, fmt.Errorf("where not equal: %w", err)
	}

	return &WhereClause{
		expression: fmt.Sprintf("%s != %s", field, v),
	}, nil
}

// WhereIn forms the field in a set expression.
func WhereIn(field string, values []interface{}) (*WhereClause, error) {
	if field == "" {
		return nil, errors.New("soql where: field can not be empty")
	}
	if values == nil {
		return nil, errors.New("soql where: value array can not be nil")
	}
	set := make([]string, len(values))
	for idx, value := range values {
		v, err := formatValue(value, AllowNil())
		if err != nil {
			return nil, fmt.Errorf("where in: %w", err)
		}
		set[idx] = v
	}

	return &WhereClause{
		expression: fmt.Sprintf("%s IN (%s)", field, strings.Join(set, ",")),
	}, nil
}

// WhereNotIn forms the field is not in a set expression.
func WhereNotIn(field string, values []interface{}) (*WhereClause, error) {
	if field == "" {
		return nil, errors.New("soql where: field can not be empty")
	}
	if values == nil {
		return nil, errors.New("soql where: value array can not be nil")
	}
	set := make([]string, len(values))
	for idx, value := range values {
		v, err := formatValue(value, AllowNil())
		if err != nil {
			return nil, fmt.Errorf("where in: %w", err)
		}
		set[idx] = v
	}

	return &WhereClause{
		expression: fmt.Sprintf("%s NOT IN (%s)", field, strings.Join(set, ",")),
	}, nil
}

// Clause returns the where cluase.
func (wc *WhereClause) Clause() string {
	return fmt.Sprintf("WHERE %s", wc.expression)
}

// Group will form a grouping around the expression.
func (wc *WhereClause) Group() {
	wc.expression = fmt.Sprintf("(%s)", wc.expression)
}

// And will logical AND the expressions.
func (wc *WhereClause) And(where WhereExpression) {
	wc.expression = fmt.Sprintf("%s AND %s", wc.expression, where.Expression())
}

// Or will logical OR the expressions.
func (wc *WhereClause) Or(where WhereExpression) {
	wc.expression = fmt.Sprintf("%s OR %s", wc.expression, where.Expression())
}

// Not will logical NOT the expressions.
func (wc *WhereClause) Not() {
	wc.expression = fmt.Sprintf("NOT %s", wc.expression)
}

// Expression will return the where expression.
func (wc *WhereClause) Expression() string {
	return wc.expression
}

// OrderResult is the type of ordering of the query result.
type OrderResult string

const (
	// OrderAsc will place the results in ascending order.
	OrderAsc OrderResult = "ASC"
	// OrderDesc will place the results in descending order.
	OrderDesc OrderResult = "DESC"
)

func (o OrderResult) IsValid() bool {
	return o == OrderAsc || o == OrderDesc
}

// OrderNulls is where the null values are placed in the ordering.
type OrderNulls string

const (
	// OrderNullsLast places the null values at the end of the ordering.
	OrderNullsLast OrderNulls = "NULLS LAST"
	// OrderNullsFirst places the null values at the start of the ordering.
	OrderNullsFirst OrderNulls = "NULLS FIRST"
)

// Orderer is the interface for returning the SOQL ordering.
type Orderer interface {
	// Order returns the order by SOQL string.
	Order() (string, error)
}

// OrderBy is the ordering structure of the SOQL query.
type OrderBy struct {
	fieldOrder []string
	result     OrderResult
	nulls      OrderNulls
}

// OrderByOpt is a function that can be used to modify the OrderBy.
type OrderByOpt func(*OrderBy)

func WithResultOrdering(result OrderResult) OrderByOpt {
	return func(o *OrderBy) {
		if result.IsValid() {
			o.result = result
		}
	}
}

func WithNullOrdering(nulls OrderNulls) OrderByOpt {
	return func(o *OrderBy) {
		o.nulls = nulls
	}
}

func ByFields(fields ...string) OrderByOpt {
	return func(o *OrderBy) {
		o.fieldOrder = fields
	}
}

// NewOrderBy creates an OrderBy structure configuring using the provided
// options.
func NewOrderBy(opts ...OrderByOpt) *OrderBy {
	o := &OrderBy{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

// AddFields appends fields to the existing ordering.
func (o *OrderBy) AddFields(fields ...string) {
	o.fieldOrder = append(o.fieldOrder, fields...)
}

// SetNullOrdering sets the ordering, first or last, of the null values.
func (o *OrderBy) SetNullOrdering(nulls OrderNulls) error {
	switch nulls {
	case OrderNullsLast, OrderNullsFirst:
	default:
		return fmt.Errorf("order by: %s is not a valid null ordering type", string(nulls))
	}
	o.nulls = nulls
	return nil
}

// Order returns the order by SOQL string.
func (o *OrderBy) Order() (string, error) {
	if len(o.fieldOrder) == 0 && o.result == "" && o.nulls == "" {
		// zero value, nothing to do
		return "", nil
	}

	switch o.result {
	case OrderAsc, OrderDesc:
	default:
		return "", fmt.Errorf("order by: %s is not a valid result ordering type", string(o.result))
	}

	if len(o.fieldOrder) == 0 {
		return "", errors.New("order by: field order can not be empty")
	}

	orderBy := "ORDER BY " + strings.Join(o.fieldOrder, ",") + " " + string(o.result)
	if o.nulls != "" {
		orderBy += " " + string(o.nulls)
	}
	return orderBy, nil
}

type formatSettings struct {
	allowEmpty bool
	allowBool  bool
	allowNil   bool
}

type formatOption func(*formatSettings)

func AllowEmpty() formatOption {
	return func(s *formatSettings) {
		s.allowEmpty = true
	}
}

func AllowBool() formatOption {
	return func(s *formatSettings) {
		s.allowBool = true
	}
}

func AllowNil() formatOption {
	return func(s *formatSettings) {
		s.allowNil = true
	}
}

// formatValue formats an argument value for use in a SOQL query.
func formatValue(value any, opts ...formatOption) (string, error) {
	settings := formatSettings{}
	for _, opt := range opts {
		opt(&settings)
	}

	if value == nil {
		if settings.allowNil {
			return "null", nil
		}
		return "", errors.New("value can not be nil")
	}

	switch val := value.(type) {
	case string:
		return fmt.Sprintf("'%s'", escapeString.Replace(val)), nil
	case bool:
		if !settings.allowBool {
			return "", errors.New("boolean is not a value set value")
		}
	case time.Time:
		return val.Format(time.RFC3339), nil
	}
	return fmt.Sprintf("%v", value), nil
}
