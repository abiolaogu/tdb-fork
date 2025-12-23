// Package tdengine provides TDengine-compatible SQL engine interface
package tdengine

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	reCreateDB       = regexp.MustCompile(`(?i)CREATE\s+DATABASE\s+(IF\s+NOT\s+EXISTS\s+)?(\w+)`)
	reDropDB         = regexp.MustCompile(`(?i)DROP\s+DATABASE\s+(IF\s+EXISTS\s+)?(\w+)`)
	reUseDB          = regexp.MustCompile(`(?i)USE\s+(\w+)`)
	rePrecision      = regexp.MustCompile(`(?i)PRECISION\s+'(\w+)'`)
	reCreateStable   = regexp.MustCompile(`(?i)CREATE\s+(?:STABLE|TABLE)\s+(IF\s+NOT\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)\s*\((.*?)\)\s*TAGS\s*\((.*?)\)`)
	reCreateTable    = regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)\s*\((.*?)\)`)
	reCreateSubTable = regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)\s+USING\s+(?:(\w+)\.)?(\w+)\s+TAGS\s*\((.*?)\)`)
	reDropTable      = regexp.MustCompile(`(?i)DROP\s+TABLE\s+(IF\s+EXISTS\s+)?(?:(\w+)\.)?(\w+)`)
	reShowTables     = regexp.MustCompile(`(?i)SHOW\s+TABLES\s*(FROM\s+(\w+))?`)
	reDescribe       = regexp.MustCompile(`(?i)DESCRIBE\s+(?:(\w+)\.)?(\w+)`)
	reCreateStream   = regexp.MustCompile(`(?i)CREATE\s+STREAM\s+(IF\s+NOT\s+EXISTS\s+)?(\w+)\s+(?:TRIGGER\s+(\w+)\s+)?(?:WATERMARK\s+(\w+)\s+)?INTO\s+(\w+)\s+AS\s+(.+)`)
	reDropStream     = regexp.MustCompile(`(?i)DROP\s+STREAM\s+(IF\s+EXISTS\s+)?(\w+)`)
)

// Engine is the TDengine SQL execution engine
type Engine struct {
	databases map[string]*Database
	users     map[string]*UserDefinition
	streams   map[string]*StreamDefinition
	topics    map[string]*TopicDefinition
	mu        sync.RWMutex

	// Default database for session
	currentDB string
}

// NewEngine creates a new TDengine engine
func NewEngine() *Engine {
	e := &Engine{
		databases: make(map[string]*Database),
		users:     make(map[string]*UserDefinition),
		streams:   make(map[string]*StreamDefinition),
		topics:    make(map[string]*TopicDefinition),
	}

	// Create default user
	e.users["root"] = &UserDefinition{
		Name:      "root",
		Password:  "taosdata", // Default TDengine password
		Privilege: "super",
		CreatedAt: time.Now(),
	}

	return e
}

// Execute executes a TDengine SQL statement
func (e *Engine) Execute(db, sql string, opts *ExecuteOptions) (*Response, error) {
	sql = strings.TrimSpace(sql)
	upperSQL := strings.ToUpper(sql)

	// Parse and route SQL
	switch {
	case strings.HasPrefix(upperSQL, "CREATE DATABASE"):
		return e.createDatabase(sql)
	case strings.HasPrefix(upperSQL, "DROP DATABASE"):
		return e.dropDatabase(sql)
	case strings.HasPrefix(upperSQL, "USE "):
		return e.useDatabase(sql)
	case strings.HasPrefix(upperSQL, "SHOW DATABASES"):
		return e.showDatabases()
	case strings.HasPrefix(upperSQL, "CREATE STABLE") || strings.HasPrefix(upperSQL, "CREATE TABLE") && strings.Contains(upperSQL, "TAGS"):
		return e.createSuperTable(db, sql)
	case strings.HasPrefix(upperSQL, "CREATE TABLE"):
		return e.createTable(db, sql)
	case strings.HasPrefix(upperSQL, "DROP TABLE"):
		return e.dropTable(db, sql)
	case strings.HasPrefix(upperSQL, "SHOW TABLES"):
		return e.showTables(db, sql)
	case strings.HasPrefix(upperSQL, "SHOW STABLES"):
		return e.showSuperTables(db)
	case strings.HasPrefix(upperSQL, "DESCRIBE"):
		return e.describeTable(db, sql)
	case strings.HasPrefix(upperSQL, "INSERT"):
		return e.insert(db, sql)
	case strings.HasPrefix(upperSQL, "SELECT"):
		return e.query(db, sql)
	case strings.HasPrefix(upperSQL, "CREATE STREAM"):
		return e.createStream(db, sql)
	case strings.HasPrefix(upperSQL, "DROP STREAM"):
		return e.dropStream(sql)
	case strings.HasPrefix(upperSQL, "SHOW STREAMS"):
		return e.showStreams()
	case strings.HasPrefix(upperSQL, "CREATE TOPIC"):
		return e.createTopic(db, sql)
	case strings.HasPrefix(upperSQL, "DROP TOPIC"):
		return e.dropTopic(sql)
	case strings.HasPrefix(upperSQL, "SHOW TOPICS"):
		return e.showTopics()
	case strings.HasPrefix(upperSQL, "ALTER"):
		return e.alterTable(db, sql)
	case strings.HasPrefix(upperSQL, "SHOW VGROUPS"):
		return e.showVgroups(db)
	case strings.HasPrefix(upperSQL, "SHOW DNODES"):
		return e.showDnodes()
	case strings.HasPrefix(upperSQL, "SHOW MNODES"):
		return e.showMnodes()
	case strings.HasPrefix(upperSQL, "SHOW USERS"):
		return e.showUsers()
	default:
		return nil, fmt.Errorf("unsupported SQL: %s", sql)
	}
}

// Authenticate validates user credentials
func (e *Engine) Authenticate(username, password string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	user, ok := e.users[username]
	if !ok {
		return false
	}

	return user.Password == password
}

// ValidateToken validates a session token
func (e *Engine) ValidateToken(token string) bool {
	// For now, accept any non-empty token
	// In production, implement proper JWT or session token validation
	return token != ""
}

// WriteInfluxDB writes data using InfluxDB line protocol
func (e *Engine) WriteInfluxDB(db string, line *InfluxDBLineProtocol) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	database, ok := e.databases[db]
	if !ok {
		return fmt.Errorf("database not found: %s", db)
	}

	// Auto-create supertable if needed
	stableName := line.Measurement
	if _, ok := database.STables[stableName]; !ok {
		// Create supertable from first data point
		schema := []Column{
			{Name: "ts", Type: TSDB_DATA_TYPE_TIMESTAMP},
		}
		for fieldName := range line.Fields {
			schema = append(schema, Column{
				Name: fieldName,
				Type: TSDB_DATA_TYPE_DOUBLE,
			})
		}

		tags := []Column{}
		for tagName := range line.Tags {
			tags = append(tags, Column{
				Name:   tagName,
				Type:   TSDB_DATA_TYPE_BINARY,
				Length: 64,
			})
		}

		database.STables[stableName] = &SuperTable{
			Name:      stableName,
			Schema:    schema,
			Tags:      tags,
			SubTables: make(map[string]*Table),
			CreatedAt: time.Now(),
		}
	}

	// Generate subtable name from tags
	tableName := generateTableName(stableName, line.Tags)

	// Auto-create subtable if needed
	stable := database.STables[stableName]
	if _, ok := stable.SubTables[tableName]; !ok {
		tagValues := make(map[string]interface{})
		for k, v := range line.Tags {
			tagValues[k] = v
		}

		stable.SubTables[tableName] = &Table{
			Name:       tableName,
			Schema:     stable.Schema,
			Tags:       tagValues,
			SuperTable: stableName,
			CreatedAt:  time.Now(),
		}
	}

	// TODO: Actually store the data point
	// For now, just validate that we can create the structures

	return nil
}

// WriteOpenTSDBJSON writes data using OpenTSDB JSON format
func (e *Engine) WriteOpenTSDBJSON(db string, point *OpenTSDBPoint) error {
	line := &InfluxDBLineProtocol{
		Measurement: point.Metric,
		Tags:        point.Tags,
		Fields:      map[string]interface{}{"value": point.Value},
		Timestamp:   point.Timestamp,
	}
	return e.WriteInfluxDB(db, line)
}

// WriteOpenTSDBTelnet writes data using OpenTSDB telnet format
func (e *Engine) WriteOpenTSDBTelnet(db, line string) error {
	// Format: put <metric> <timestamp> <value> <tagk1>=<tagv1> ...
	parts := strings.Fields(line)
	if len(parts) < 4 || parts[0] != "put" {
		return fmt.Errorf("invalid OpenTSDB telnet format")
	}

	metric := parts[1]
	timestamp, _ := strconv.ParseInt(parts[2], 10, 64)
	value, _ := strconv.ParseFloat(parts[3], 64)

	tags := make(map[string]string)
	for _, tag := range parts[4:] {
		kv := strings.SplitN(tag, "=", 2)
		if len(kv) == 2 {
			tags[kv[0]] = kv[1]
		}
	}

	point := &OpenTSDBPoint{
		Metric:    metric,
		Timestamp: timestamp,
		Value:     value,
		Tags:      tags,
	}

	return e.WriteOpenTSDBJSON(db, point)
}

// Database operations
func (e *Engine) createDatabase(sql string) (*Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Parse: CREATE DATABASE [IF NOT EXISTS] <name> [options...]
	matches := reCreateDB.FindStringSubmatch(sql)
	if len(matches) < 3 {
		return nil, fmt.Errorf("invalid CREATE DATABASE syntax")
	}

	ifNotExists := matches[1] != ""
	name := matches[2]

	if _, exists := e.databases[name]; exists {
		if ifNotExists {
			return &Response{Code: TSDB_CODE_SUCCESS}, nil
		}
		return nil, fmt.Errorf("database already exists: %s", name)
	}

	// Parse options
	precision := "ms"
	if strings.Contains(strings.ToUpper(sql), "PRECISION") {
		if m := rePrecision.FindStringSubmatch(sql); len(m) > 1 {
			precision = m[1]
		}
	}

	e.databases[name] = &Database{
		Name:      name,
		Precision: precision,
		STables:   make(map[string]*SuperTable),
		Tables:    make(map[string]*Table),
		CreatedAt: time.Now(),
	}

	return &Response{Code: TSDB_CODE_SUCCESS}, nil
}

func (e *Engine) dropDatabase(sql string) (*Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	matches := reDropDB.FindStringSubmatch(sql)
	if len(matches) < 3 {
		return nil, fmt.Errorf("invalid DROP DATABASE syntax")
	}

	ifExists := matches[1] != ""
	name := matches[2]

	if _, exists := e.databases[name]; !exists {
		if ifExists {
			return &Response{Code: TSDB_CODE_SUCCESS}, nil
		}
		return nil, fmt.Errorf("database not found: %s", name)
	}

	delete(e.databases, name)
	return &Response{Code: TSDB_CODE_SUCCESS}, nil
}

func (e *Engine) useDatabase(sql string) (*Response, error) {
	matches := reUseDB.FindStringSubmatch(sql)
	if len(matches) < 2 {
		return nil, fmt.Errorf("invalid USE syntax")
	}

	name := matches[1]

	e.mu.RLock()
	_, exists := e.databases[name]
	e.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("database not found: %s", name)
	}

	e.currentDB = name
	return &Response{Code: TSDB_CODE_SUCCESS}, nil
}

func (e *Engine) showDatabases() (*Response, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	data := make([][]interface{}, 0, len(e.databases))
	for name, db := range e.databases {
		data = append(data, []interface{}{
			name,
			db.CreatedAt.Format("2006-01-02 15:04:05.000"),
			len(db.STables),
			db.Precision,
		})
	}

	return &Response{
		Code: TSDB_CODE_SUCCESS,
		ColumnMeta: [][]interface{}{
			{"name", TSDB_DATA_TYPE_BINARY, 64},
			{"created_time", TSDB_DATA_TYPE_TIMESTAMP, 8},
			{"ntables", TSDB_DATA_TYPE_INT, 4},
			{"precision", TSDB_DATA_TYPE_BINARY, 8},
		},
		Data: data,
		Rows: len(data),
	}, nil
}

func (e *Engine) createSuperTable(db, sql string) (*Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	database := e.getDatabase(db)
	if database == nil {
		return nil, fmt.Errorf("database not found: %s", db)
	}

	// Parse CREATE STABLE syntax
	// CREATE STABLE [IF NOT EXISTS] [db.]name (columns) TAGS (tags)
	// Create SuperTable - reCreateStable
	matches := reCreateStable.FindStringSubmatch(sql)
	if len(matches) < 6 {
		return nil, fmt.Errorf("invalid CREATE STABLE syntax")
	}

	ifNotExists := matches[1] != ""
	if matches[2] != "" {
		db = matches[2]
		database = e.databases[db]
		if database == nil {
			return nil, fmt.Errorf("database not found: %s", db)
		}
	}
	name := matches[3]
	columnsStr := matches[4]
	tagsStr := matches[5]

	if _, exists := database.STables[name]; exists {
		if ifNotExists {
			return &Response{Code: TSDB_CODE_SUCCESS}, nil
		}
		return nil, fmt.Errorf("supertable already exists: %s", name)
	}

	schema := parseColumns(columnsStr, false)
	tags := parseColumns(tagsStr, true)

	database.STables[name] = &SuperTable{
		Name:      name,
		Schema:    schema,
		Tags:      tags,
		SubTables: make(map[string]*Table),
		CreatedAt: time.Now(),
	}

	return &Response{Code: TSDB_CODE_SUCCESS}, nil
}

func (e *Engine) createTable(db, sql string) (*Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	database := e.getDatabase(db)
	if database == nil {
		return nil, fmt.Errorf("database not found: %s", db)
	}

	upperSQL := strings.ToUpper(sql)

	// Check if it's a subtable creation: CREATE TABLE name USING stable TAGS (values)
	if strings.Contains(upperSQL, "USING") {
		matches := reCreateSubTable.FindStringSubmatch(sql)
		if len(matches) < 7 {
			return nil, fmt.Errorf("invalid CREATE TABLE ... USING syntax")
		}

		ifNotExists := matches[1] != ""
		if matches[2] != "" {
			db = matches[2]
			database = e.databases[db]
		}
		tableName := matches[3]
		stableDB := matches[4]
		if stableDB == "" {
			stableDB = db
		}
		stableName := matches[5]
		tagValuesStr := matches[6]

		stableDatabase := e.databases[stableDB]
		if stableDatabase == nil {
			return nil, fmt.Errorf("database not found: %s", stableDB)
		}

		stable, ok := stableDatabase.STables[stableName]
		if !ok {
			return nil, fmt.Errorf("supertable not found: %s", stableName)
		}

		if _, exists := stable.SubTables[tableName]; exists {
			if ifNotExists {
				return &Response{Code: TSDB_CODE_SUCCESS}, nil
			}
			return nil, fmt.Errorf("table already exists: %s", tableName)
		}

		tagValues := parseTagValues(tagValuesStr, stable.Tags)

		stable.SubTables[tableName] = &Table{
			Name:       tableName,
			Schema:     stable.Schema,
			Tags:       tagValues,
			SuperTable: stableName,
			CreatedAt:  time.Now(),
		}

		return &Response{Code: TSDB_CODE_SUCCESS}, nil
	}

	// Regular table (not subtable)
	matches := reCreateTable.FindStringSubmatch(sql)
	if len(matches) < 5 {
		return nil, fmt.Errorf("invalid CREATE TABLE syntax")
	}

	ifNotExists := matches[1] != ""
	if matches[2] != "" {
		db = matches[2]
		database = e.databases[db]
	}
	name := matches[3]
	columnsStr := matches[4]

	if _, exists := database.Tables[name]; exists {
		if ifNotExists {
			return &Response{Code: TSDB_CODE_SUCCESS}, nil
		}
		return nil, fmt.Errorf("table already exists: %s", name)
	}

	schema := parseColumns(columnsStr, false)

	database.Tables[name] = &Table{
		Name:      name,
		Schema:    schema,
		CreatedAt: time.Now(),
	}

	return &Response{Code: TSDB_CODE_SUCCESS}, nil
}

func (e *Engine) dropTable(db, sql string) (*Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	database := e.getDatabase(db)
	if database == nil {
		return nil, fmt.Errorf("database not found: %s", db)
	}

	matches := reDropTable.FindStringSubmatch(sql)
	if len(matches) < 4 {
		return nil, fmt.Errorf("invalid DROP TABLE syntax")
	}

	ifExists := matches[1] != ""
	if matches[2] != "" {
		db = matches[2]
		database = e.databases[db]
	}
	name := matches[3]

	// Check regular tables
	if _, exists := database.Tables[name]; exists {
		delete(database.Tables, name)
		return &Response{Code: TSDB_CODE_SUCCESS}, nil
	}

	// Check subtables in all supertables
	for _, stable := range database.STables {
		if _, exists := stable.SubTables[name]; exists {
			delete(stable.SubTables, name)
			return &Response{Code: TSDB_CODE_SUCCESS}, nil
		}
	}

	if ifExists {
		return &Response{Code: TSDB_CODE_SUCCESS}, nil
	}

	return nil, fmt.Errorf("table not found: %s", name)
}

func (e *Engine) showTables(db, sql string) (*Response, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Parse optional FROM clause
	matches := reShowTables.FindStringSubmatch(sql)
	if len(matches) > 2 && matches[2] != "" {
		db = matches[2]
	}

	database := e.getDatabase(db)
	if database == nil {
		return nil, fmt.Errorf("database not found: %s", db)
	}

	data := make([][]interface{}, 0)

	// Regular tables
	for name, table := range database.Tables {
		data = append(data, []interface{}{
			name,
			table.CreatedAt.Format("2006-01-02 15:04:05.000"),
			len(table.Schema),
			"",
		})
	}

	// Subtables
	for stableName, stable := range database.STables {
		for name, table := range stable.SubTables {
			data = append(data, []interface{}{
				name,
				table.CreatedAt.Format("2006-01-02 15:04:05.000"),
				len(table.Schema),
				stableName,
			})
		}
	}

	return &Response{
		Code: TSDB_CODE_SUCCESS,
		ColumnMeta: [][]interface{}{
			{"table_name", TSDB_DATA_TYPE_BINARY, 192},
			{"created_time", TSDB_DATA_TYPE_TIMESTAMP, 8},
			{"columns", TSDB_DATA_TYPE_INT, 4},
			{"stable_name", TSDB_DATA_TYPE_BINARY, 192},
		},
		Data: data,
		Rows: len(data),
	}, nil
}

func (e *Engine) showSuperTables(db string) (*Response, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	database := e.getDatabase(db)
	if database == nil {
		return nil, fmt.Errorf("database not found: %s", db)
	}

	data := make([][]interface{}, 0, len(database.STables))
	for name, stable := range database.STables {
		data = append(data, []interface{}{
			name,
			stable.CreatedAt.Format("2006-01-02 15:04:05.000"),
			len(stable.Schema),
			len(stable.Tags),
			len(stable.SubTables),
		})
	}

	return &Response{
		Code: TSDB_CODE_SUCCESS,
		ColumnMeta: [][]interface{}{
			{"stable_name", TSDB_DATA_TYPE_BINARY, 192},
			{"created_time", TSDB_DATA_TYPE_TIMESTAMP, 8},
			{"columns", TSDB_DATA_TYPE_INT, 4},
			{"tags", TSDB_DATA_TYPE_INT, 4},
			{"tables", TSDB_DATA_TYPE_INT, 4},
		},
		Data: data,
		Rows: len(data),
	}, nil
}

func (e *Engine) describeTable(db, sql string) (*Response, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	matches := reDescribe.FindStringSubmatch(sql)
	if len(matches) < 3 {
		return nil, fmt.Errorf("invalid DESCRIBE syntax")
	}

	if matches[1] != "" {
		db = matches[1]
	}
	name := matches[2]

	database := e.getDatabase(db)
	if database == nil {
		return nil, fmt.Errorf("database not found: %s", db)
	}

	// Check supertables
	if stable, ok := database.STables[name]; ok {
		return e.describeColumns(stable.Schema, stable.Tags), nil
	}

	// Check regular tables
	if table, ok := database.Tables[name]; ok {
		return e.describeColumns(table.Schema, nil), nil
	}

	// Check subtables
	for _, stable := range database.STables {
		if table, ok := stable.SubTables[name]; ok {
			return e.describeColumns(table.Schema, stable.Tags), nil
		}
	}

	return nil, fmt.Errorf("table not found: %s", name)
}

func (e *Engine) describeColumns(schema, tags []Column) *Response {
	data := make([][]interface{}, 0, len(schema)+len(tags))

	for _, col := range schema {
		data = append(data, []interface{}{
			col.Name,
			typeToString(col.Type),
			col.Length,
			"",
		})
	}

	for _, tag := range tags {
		data = append(data, []interface{}{
			tag.Name,
			typeToString(tag.Type),
			tag.Length,
			"TAG",
		})
	}

	return &Response{
		Code: TSDB_CODE_SUCCESS,
		ColumnMeta: [][]interface{}{
			{"Field", TSDB_DATA_TYPE_BINARY, 64},
			{"Type", TSDB_DATA_TYPE_BINARY, 16},
			{"Length", TSDB_DATA_TYPE_INT, 4},
			{"Note", TSDB_DATA_TYPE_BINARY, 12},
		},
		Data: data,
		Rows: len(data),
	}
}

func (e *Engine) insert(db, sql string) (*Response, error) {
	// For now, just parse and count affected rows
	// TODO: Actual data storage integration

	// Count VALUES clauses to determine affected rows
	valuesCount := strings.Count(strings.ToUpper(sql), "(") - 1 // Subtract table definition
	if valuesCount < 0 {
		valuesCount = 0
	}
	if valuesCount == 0 {
		valuesCount = 1
	}

	return &Response{
		Code:         TSDB_CODE_SUCCESS,
		AffectedRows: valuesCount,
	}, nil
}

func (e *Engine) query(db, sql string) (*Response, error) {
	// Simplified query implementation
	// TODO: Full SQL parsing and execution

	// For now, return empty result set
	return &Response{
		Code: TSDB_CODE_SUCCESS,
		ColumnMeta: [][]interface{}{
			{"ts", TSDB_DATA_TYPE_TIMESTAMP, 8},
			{"value", TSDB_DATA_TYPE_DOUBLE, 8},
		},
		Data: [][]interface{}{},
		Rows: 0,
	}, nil
}

func (e *Engine) createStream(db, sql string) (*Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Parse CREATE STREAM syntax
	matches := reCreateStream.FindStringSubmatch(sql)
	if len(matches) < 7 {
		return nil, fmt.Errorf("invalid CREATE STREAM syntax")
	}

	name := matches[2]
	trigger := matches[3]
	if trigger == "" {
		trigger = "at_once"
	}
	watermark := matches[4]
	targetTable := matches[5]
	selectSQL := matches[6]

	e.streams[name] = &StreamDefinition{
		Name:        name,
		TargetTable: targetTable,
		SQL:         selectSQL,
		Trigger:     trigger,
		Watermark:   watermark,
		CreatedAt:   time.Now(),
	}

	return &Response{Code: TSDB_CODE_SUCCESS}, nil
}

func (e *Engine) dropStream(sql string) (*Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	matches := reDropStream.FindStringSubmatch(sql)
	if len(matches) < 3 {
		return nil, fmt.Errorf("invalid DROP STREAM syntax")
	}

	ifExists := matches[1] != ""
	name := matches[2]

	if _, exists := e.streams[name]; !exists {
		if ifExists {
			return &Response{Code: TSDB_CODE_SUCCESS}, nil
		}
		return nil, fmt.Errorf("stream not found: %s", name)
	}

	delete(e.streams, name)
	return &Response{Code: TSDB_CODE_SUCCESS}, nil
}

func (e *Engine) showStreams() (*Response, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	data := make([][]interface{}, 0, len(e.streams))
	for name, stream := range e.streams {
		data = append(data, []interface{}{
			name,
			stream.TargetTable,
			stream.Trigger,
			stream.SQL,
		})
	}

	return &Response{
		Code: TSDB_CODE_SUCCESS,
		ColumnMeta: [][]interface{}{
			{"stream_name", TSDB_DATA_TYPE_BINARY, 192},
			{"target_table", TSDB_DATA_TYPE_BINARY, 192},
			{"trigger", TSDB_DATA_TYPE_BINARY, 16},
			{"sql", TSDB_DATA_TYPE_BINARY, 1024},
		},
		Data: data,
		Rows: len(data),
	}, nil
}

func (e *Engine) createTopic(db, sql string) (*Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	re := regexp.MustCompile(`(?i)CREATE\s+TOPIC\s+(IF\s+NOT\s+EXISTS\s+)?(\w+)\s+(?:WITH\s+META\s+)?AS\s+(.+)`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) < 4 {
		return nil, fmt.Errorf("invalid CREATE TOPIC syntax")
	}

	name := matches[2]
	selectSQL := matches[3]
	withMeta := strings.Contains(strings.ToUpper(sql), "WITH META")

	e.topics[name] = &TopicDefinition{
		Name:      name,
		Database:  db,
		SQL:       selectSQL,
		WithMeta:  withMeta,
		CreatedAt: time.Now(),
	}

	return &Response{Code: TSDB_CODE_SUCCESS}, nil
}

func (e *Engine) dropTopic(sql string) (*Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	re := regexp.MustCompile(`(?i)DROP\s+TOPIC\s+(IF\s+EXISTS\s+)?(\w+)`)
	matches := re.FindStringSubmatch(sql)
	if len(matches) < 3 {
		return nil, fmt.Errorf("invalid DROP TOPIC syntax")
	}

	ifExists := matches[1] != ""
	name := matches[2]

	if _, exists := e.topics[name]; !exists {
		if ifExists {
			return &Response{Code: TSDB_CODE_SUCCESS}, nil
		}
		return nil, fmt.Errorf("topic not found: %s", name)
	}

	delete(e.topics, name)
	return &Response{Code: TSDB_CODE_SUCCESS}, nil
}

func (e *Engine) showTopics() (*Response, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	data := make([][]interface{}, 0, len(e.topics))
	for name, topic := range e.topics {
		data = append(data, []interface{}{
			name,
			topic.Database,
			topic.SQL,
		})
	}

	return &Response{
		Code: TSDB_CODE_SUCCESS,
		ColumnMeta: [][]interface{}{
			{"topic_name", TSDB_DATA_TYPE_BINARY, 192},
			{"database", TSDB_DATA_TYPE_BINARY, 64},
			{"sql", TSDB_DATA_TYPE_BINARY, 1024},
		},
		Data: data,
		Rows: len(data),
	}, nil
}

func (e *Engine) alterTable(db, sql string) (*Response, error) {
	// TODO: Implement ALTER TABLE
	return &Response{Code: TSDB_CODE_SUCCESS}, nil
}

func (e *Engine) showVgroups(db string) (*Response, error) {
	// Simulate single vgroup for now
	return &Response{
		Code: TSDB_CODE_SUCCESS,
		ColumnMeta: [][]interface{}{
			{"vgroup_id", TSDB_DATA_TYPE_INT, 4},
			{"database", TSDB_DATA_TYPE_BINARY, 64},
			{"tables", TSDB_DATA_TYPE_INT, 4},
			{"status", TSDB_DATA_TYPE_BINARY, 16},
		},
		Data: [][]interface{}{
			{1, db, 0, "ready"},
		},
		Rows: 1,
	}, nil
}

func (e *Engine) showDnodes() (*Response, error) {
	return &Response{
		Code: TSDB_CODE_SUCCESS,
		ColumnMeta: [][]interface{}{
			{"id", TSDB_DATA_TYPE_INT, 4},
			{"endpoint", TSDB_DATA_TYPE_BINARY, 128},
			{"status", TSDB_DATA_TYPE_BINARY, 16},
		},
		Data: [][]interface{}{
			{1, "localhost:6030", "ready"},
		},
		Rows: 1,
	}, nil
}

func (e *Engine) showMnodes() (*Response, error) {
	return &Response{
		Code: TSDB_CODE_SUCCESS,
		ColumnMeta: [][]interface{}{
			{"id", TSDB_DATA_TYPE_INT, 4},
			{"endpoint", TSDB_DATA_TYPE_BINARY, 128},
			{"role", TSDB_DATA_TYPE_BINARY, 16},
		},
		Data: [][]interface{}{
			{1, "localhost:6030", "leader"},
		},
		Rows: 1,
	}, nil
}

func (e *Engine) showUsers() (*Response, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	data := make([][]interface{}, 0, len(e.users))
	for name, user := range e.users {
		data = append(data, []interface{}{
			name,
			user.Privilege,
			user.CreatedAt.Format("2006-01-02 15:04:05.000"),
		})
	}

	return &Response{
		Code: TSDB_CODE_SUCCESS,
		ColumnMeta: [][]interface{}{
			{"name", TSDB_DATA_TYPE_BINARY, 64},
			{"privilege", TSDB_DATA_TYPE_BINARY, 16},
			{"created_time", TSDB_DATA_TYPE_TIMESTAMP, 8},
		},
		Data: data,
		Rows: len(data),
	}, nil
}

// Helper functions

func (e *Engine) getDatabase(name string) *Database {
	if name == "" {
		name = e.currentDB
	}
	return e.databases[name]
}

func generateTableName(stable string, tags map[string]string) string {
	// Generate deterministic table name from tags
	var parts []string
	for _, v := range tags {
		parts = append(parts, v)
	}
	if len(parts) == 0 {
		return stable + "_0"
	}
	return stable + "_" + strings.Join(parts, "_")
}

func parseColumns(columnsStr string, isTags bool) []Column {
	columns := []Column{}
	parts := strings.Split(columnsStr, ",")

	for _, part := range parts {
		part = strings.TrimSpace(part)
		fields := strings.Fields(part)
		if len(fields) < 2 {
			continue
		}

		name := fields[0]
		typeStr := strings.ToUpper(fields[1])
		length := 0

		// Parse length for BINARY/NCHAR
		if strings.Contains(typeStr, "(") {
			re := regexp.MustCompile(`(\w+)\((\d+)\)`)
			matches := re.FindStringSubmatch(typeStr)
			if len(matches) >= 3 {
				typeStr = matches[1]
				length, _ = strconv.Atoi(matches[2])
			}
		}

		columns = append(columns, Column{
			Name:   name,
			Type:   stringToType(typeStr),
			Length: length,
			IsTag:  isTags,
		})
	}

	return columns
}

func parseTagValues(valuesStr string, tagDefs []Column) map[string]interface{} {
	values := make(map[string]interface{})
	parts := strings.Split(valuesStr, ",")

	for i, part := range parts {
		if i >= len(tagDefs) {
			break
		}
		part = strings.TrimSpace(part)
		part = strings.Trim(part, "'\"")
		values[tagDefs[i].Name] = part
	}

	return values
}

func stringToType(s string) int {
	switch s {
	case "TIMESTAMP":
		return TSDB_DATA_TYPE_TIMESTAMP
	case "BOOL":
		return TSDB_DATA_TYPE_BOOL
	case "TINYINT":
		return TSDB_DATA_TYPE_TINYINT
	case "SMALLINT":
		return TSDB_DATA_TYPE_SMALLINT
	case "INT":
		return TSDB_DATA_TYPE_INT
	case "BIGINT":
		return TSDB_DATA_TYPE_BIGINT
	case "FLOAT":
		return TSDB_DATA_TYPE_FLOAT
	case "DOUBLE":
		return TSDB_DATA_TYPE_DOUBLE
	case "BINARY":
		return TSDB_DATA_TYPE_BINARY
	case "NCHAR":
		return TSDB_DATA_TYPE_NCHAR
	case "JSON":
		return TSDB_DATA_TYPE_JSON
	default:
		return TSDB_DATA_TYPE_BINARY
	}
}

func typeToString(t int) string {
	switch t {
	case TSDB_DATA_TYPE_TIMESTAMP:
		return "TIMESTAMP"
	case TSDB_DATA_TYPE_BOOL:
		return "BOOL"
	case TSDB_DATA_TYPE_TINYINT:
		return "TINYINT"
	case TSDB_DATA_TYPE_SMALLINT:
		return "SMALLINT"
	case TSDB_DATA_TYPE_INT:
		return "INT"
	case TSDB_DATA_TYPE_BIGINT:
		return "BIGINT"
	case TSDB_DATA_TYPE_FLOAT:
		return "FLOAT"
	case TSDB_DATA_TYPE_DOUBLE:
		return "DOUBLE"
	case TSDB_DATA_TYPE_BINARY:
		return "BINARY"
	case TSDB_DATA_TYPE_NCHAR:
		return "NCHAR"
	case TSDB_DATA_TYPE_JSON:
		return "JSON"
	default:
		return "UNKNOWN"
	}
}
