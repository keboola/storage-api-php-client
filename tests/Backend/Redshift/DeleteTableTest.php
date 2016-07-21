<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 03/05/16
 * Time: 09:45
 */
namespace Keboola\Test\Backend\Redshift;

use Keboola\Test\StorageApiTestCase;
use Keboola\Csv\CsvFile;

class DeleteTableTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testRedshiftTableDropWithViewShouldReturnDependencies()
    {
        $token = $this->_client->verifyToken();
        $dbh = $this->getDb($token);
        $dbh->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        $workingSchemaName = sprintf('tapi_%d_sand', $token['id']);
        $stmt = $dbh->prepare("SELECT * FROM pg_catalog.pg_namespace WHERE nspname = ?");
        $stmt->execute(array($workingSchemaName));
        $schema = $stmt->fetch();

        if (!$schema) {
            $dbh->query('CREATE SCHEMA ' . $workingSchemaName);
        }

        $stmt = $dbh->prepare("SELECT table_name FROM information_schema.views WHERE table_schema = ?");
        $stmt->execute(array($workingSchemaName));
        while ($table = $stmt->fetch()) {
            $dbh->query("DROP VIEW $workingSchemaName." . '"' . $table['table_name'] . '"');
        }

        $testBucketId = $this->getTestBucketId(self::STAGE_IN);
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $sourceTableId = $this->_client->createTable(
            $testBucketId,
            'languages',
            new CsvFile($importFile),
            array(
                'primaryKey' => 'id',
            )
        );
        $dbh->query("CREATE VIEW \"$workingSchemaName\".languages AS SELECT * FROM \"$testBucketId\".languages");

        try {
            $this->_client->dropTable($sourceTableId);
            $this->fail('Delete should not be allowed');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.dependentObjects', $e->getStringCode());
            $this->assertEquals([['id' => $token['id'], 'description' => $token['description']]], $e->getContextParams()['params']['dependencies']['sandbox']);
        }
        $dbh->query("DROP VIEW  \"$workingSchemaName\".languages");
    }

    /**
     * @return \PDO
     */
    private function getDb($token)
    {
        return new \PDO(
            "pgsql:dbname={$token['owner']['redshift']['databaseName']};port=5439;host=" . REDSHIFT_HOSTNAME,
            REDSHIFT_USER,
            REDSHIFT_PASSWORD
        );
    }

}