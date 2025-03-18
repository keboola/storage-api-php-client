<?php

declare(strict_types=1);

namespace Keboola\Test\Utils;

use Keboola\StorageApi\Client;

/**
 * @phpstan-import-type GlobalSearchResult from Client
 */
trait GlobalSearchTesterUtils
{
    use EventTesterUtils;

    public function assertGlobalSearchTable(
        Client $client,
        string $expectedTableName,
        int $expectedProjectId,
        int|null $expectedBranchId = null,
    ): void {
        $apiCall = fn() => $client->globalSearch($expectedTableName);
        $assertCallback = function ($searchResult) use ($expectedTableName, $expectedProjectId, $expectedBranchId) {
            $items = $this->filterSearchResultsByProjectAndBranch(
                $searchResult,
                $expectedProjectId,
                $expectedBranchId,
            );
            $this->assertIsArray($items);
            $items = array_filter($items, fn(array $item) => $item['type'] === 'table');
            $this->assertIsArray($items);
            reset($items);
            $this->assertCount(1, $items, $this->getGlobalSearchErrorMsg('No table not found, 1 expected', $searchResult));
            $this->assertSame($expectedTableName, $items[0]['name'], $this->getGlobalSearchErrorMsg(sprintf('Table "%s" not found', $expectedTableName), $searchResult));
        };
        $this->retryWithCallback($apiCall, $assertCallback);
    }

    /**
     * @param GlobalSearchResult $result
     * @return array<mixed>
     */
    public function filterSearchResultsByProjectAndBranch(
        array $result,
        int $projectId,
        int|null $branchId = null
    ): array {
        $items = $result['items'];
        $this->assertIsArray($items);
        $items = array_filter($items, function (array $item) use ($projectId, $branchId): bool {
            if (!array_key_exists('projectId', $item)) {
                return false;
            }
            if ($branchId !== null) {
                if (!array_key_exists('fullPath', $item)) {
                    return false;
                }
                if (!array_key_exists('branch', $item['fullPath'])) {
                    return false;
                }
                if (!array_key_exists('id', $item['fullPath']['branch'])) {
                    return false;
                }

                return $item['projectId'] === $projectId && $item['fullPath']['branch']['id'] === $branchId;
            }
            return $item['projectId'] === $projectId;
        });
        reset($items);
        return $items;
    }

    /**
     * @param array<mixed> $results
     */
    public function getGlobalSearchErrorMsg(string $error, array $results): string
    {
        return sprintf('Global search assert failed: %s [%s]', $error, json_encode($results));
    }
}
