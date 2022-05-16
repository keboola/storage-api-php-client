<?php
namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\Test\StorageApiTestCase;

class HandleAsyncTasksTest extends StorageApiTestCase
{
    public function testWriteTableAsyncSuccess(): void
    {
        $jobResult1 = array (
            'id' => 1,
            'status' => 'success'
        );
        $jobResult2 = array (
            'id' => 2,
            'status' => 'success'
        );

        $clientMock = self::getMockBuilder(Client::class)
            ->disableOriginalConstructor()
            ->setMethods(['waitForJob'])
            ->getMock();
        $clientMock->expects(self::any())
            ->method('waitForJob')
            ->withConsecutive([1], [2])
            ->willReturnOnConsecutiveCalls($jobResult1, $jobResult2);

        $results = $clientMock->handleAsyncTasks([1, 2]);
        $this->assertCount(2, $results);
        $this->assertEquals([$jobResult1, $jobResult2], $results);
    }

    public function testWriteTableAsyncError(): void
    {
        $jobResult1 = array (
            'id' => 1,
            'status' => 'success',
        );
        $jobResult2 = array (
            'id' => 2,
            'status' => 'error',
            'error' =>
                array (
                    'code' => 'invalidData',
                    'message' => 'errorMessage',
                    'exceptionId' => 'keboola-connection-abcdef0123456789',
                )
        );

        $clientMock = self::getMockBuilder(Client::class)
            ->disableOriginalConstructor()
            ->setMethods(['waitForJob'])
            ->getMock();
        $clientMock->expects(self::any())
            ->method('waitForJob')
            ->withConsecutive([1], [2])
            ->willReturnOnConsecutiveCalls($jobResult1, $jobResult2);

        try {
            $clientMock->handleAsyncTasks([1, 2]);
            $this->fail('Missing exception');
        } catch (ClientException $e) {
            $this->assertStringContainsString('invalidData', $e->getStringCode());
        }
    }
}
