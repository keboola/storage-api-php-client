<?php

namespace Keboola\Test\Backend\Workspaces\Backend;

class RedshiftWorkspaceBackend implements WorkspaceBackend {

    private $db;
    
    private $schema;

    private function getDbConnection($connection) 
    {
        $pdo = new \PDO(
            "pgsql:dbname={$connection['database']};port=5439;host=" . $connection['host'],
            $connection['user'],
            $connection['password']
        );
        $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        return $pdo;
    }
    
    public function __construct($workspace) 
    {
        $this->db = $this->getDbConnection($workspace['connection']);
        $this->schema = $workspace['connection']['schema'];
    }
    
    public function getTableColumns($table)
    {
        $stmt = $this->db->prepare("SELECT \"column\" FROM PG_TABLE_DEF WHERE tablename = ?;");
        $stmt->execute(array($table));
        return array_map(function ($row) {
                return $row['column'];
            },  $stmt->fetchAll()
        );
    }

    public function getTables()
    {
        $stmt = $this->db->prepare("select tablename from PG_TABLES where schemaname = ?");
        $stmt->execute(array($this->schema));
        return array_map(function ($table) {
                return $table['tablename'];
            },  $stmt->fetchAll()
        );
    }

    public function dropTable($table)
    {
        $this->db->query(sprintf("DROP TABLE \"{$this->schema}\".\"%s\";", $table));
    }

    public function countRows($table)
    {
        $stmt = $this->db->prepare(sprintf("select count(*) as count from \"{$this->schema}\".\"%s\"", $table));
        $stmt->execute();
        $count = $stmt->fetch();
        return $count['count'];
    }

    public function fetchAll($table, $style = \PDO::FETCH_NUM)
    {
        $stmt = $this->db->prepare(sprintf("SELECT * FROM \"{$this->schema}\".\"%s\"", $table));
        $stmt->execute();
        return $stmt->fetchAll($style);
    }

    public function createTable($tableName, $columns) {
        $cols = [];
        foreach($columns as $column => $datatype) {
            $cols[] = "\"{$column}\" {$datatype}";
        }
        $definition = join(",\n", $cols);

        $this->db->query(
            sprintf("CREATE TABLE %s (%s)", $tableName, $definition)
        );
    }

    public function toIdentifier($item)
    {
        if (is_array($item)) {
            return array_map(function ($item) {
                return strtolower($item);
            }, $item);
        } else {
            return strtolower($item);
        }
    }
}