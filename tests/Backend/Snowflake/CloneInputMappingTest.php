<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;
use Keboola\Test\StorageApiTestCase;

class CloneInputMappingTest extends WorkspacesTestCase
{
    const IMPORT_FILE_PATH = __DIR__ . '/../../_data/languages.csv';

    public function testCloneInputMapping(): void
    {
        $bucketId = $this->ensureBucket($this->_client, 'clone-input-mapping');
        $tableId = $this->createTableFromFile(
            $this->_client,
            $bucketId,
            self::IMPORT_FILE_PATH
        );

        $this->runAndAssertWorkspaceClone($tableId);
    }

    public function testCloneLinkedBucket(): void
    {
        $client2 = new \Keboola\StorageApi\Client([
            'token' => STORAGE_API_LINKING_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
        ]);

        $this->_client->dropBucket('in.c-clone-input-mapping', ['force' => true]);
        $bucketId = $this->ensureBucket($client2, 'c-linked-to-dev');
        $this->createTableFromFile(
            $client2,
            $bucketId,
            self::IMPORT_FILE_PATH
        );
        $client2->shareBucket($bucketId);
        $projectId = $client2->verifyToken()['owner']['id'];

        $bucketId = $this->_client->linkBucket('clone-input-mapping', 'in', $projectId, $bucketId);

        $this->runAndAssertWorkspaceClone($bucketId . '.languagesDetails');
    }

    public function testCloneSimpleAlias(): void
    {
        $bucketId = $this->ensureBucket($this->_client, 'clone-alias-bucket', 'in');
        $sourceTableId = $this->createTableFromFile(
            $this->_client,
            $bucketId,
            self::IMPORT_FILE_PATH
        );
        $bucketId = $this->ensureBucket($this->_client, 'clone-input-mapping');
        $tableId = $this->_client->createAliasTable($bucketId, $sourceTableId);

        $this->runAndAssertWorkspaceClone($tableId);
    }

    private function ensureBucket(
        Client $client,
        string $bucketName,
        string $stage = 'in'
    ): string {
        if ($client->bucketExists($stage . '.' . 'c-' . $bucketName)) {
            $client->dropBucket(
                $stage . '.' . 'c-' . $bucketName,
                [
                    'force' => true,
                ]
            );
        }
        return $client->createBucket($bucketName, $stage);
    }

    /**
     * @param array|string $primaryKey
     */
    private function createTableFromFile(
        Client $client,
        string $bucketId,
        string $importFilePath,
        $primaryKey = 'id'
    ): string {
        return $client->createTable(
            $bucketId,
            'languagesDetails',
            new CsvFile($importFilePath),
            ['primaryKey' => $primaryKey]
        );
    }

    /**
     * @param $tableId
     * @param $workspacesClient
     * @param $workspace
     */
    private function runAndAssertWorkspaceClone($tableId): void
    {
        $workspacesClient = new Workspaces($this->_client);

        $workspace = $workspacesClient->createWorkspace([
            'name' => 'clone',
        ]);

        $workspacesClient->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languagesDetails',
                ],
            ],
        ]);

        $data = $this->_client->getTableDataPreview($tableId);
        $rows = str_getcsv($data, "\n");
        $result = array_map('str_getcsv', $rows);

        $this->assertSame([
            [
                'id',
                'name',
            ],
            [
                '0',
                '- unchecked -',
            ],
            [
                '11',
                'finnish',
            ],
            [
                '24',
                'french',
            ],
            [
                '26',
                'czech',
            ],
            [
                '1',
                'english',
            ],
        ], $result);
    }
}
