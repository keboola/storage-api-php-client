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
        $stmt = $this->db->prepare("select column_name from information_schema.columns WHERE table_schema = \"{$this->schema}\" AND table_name = ?;");
        $stmt->execute(array($table));
        return array_map(function ($column) {
                return $column['column_name'];
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
        $this->db->query(sprintf("DROP TABLE \"{$this->schema}\".%s;", $table));
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