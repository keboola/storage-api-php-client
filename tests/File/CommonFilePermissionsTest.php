<?php

declare(strict_types=1);

namespace Keboola\Test\File;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\Options\TokenUpdateOptions;
use Keboola\Test\StorageApiTestCase;

class CommonFilePermissionsTest extends StorageApiTestCase
{
    public function testFilesPermissions(): void
    {
        $filePath = __DIR__ . '/../_data/files.upload.txt';
        $uploadOptions = new FileUploadOptions();

        $tokenOptions = (new TokenCreateOptions())
            ->setDescription('Files test')
        ;

        $newToken = $this->tokens->createToken($tokenOptions);

        $this->createAndWaitForFile($filePath, $uploadOptions);

        $totalFilesCount = count($this->_client->listFiles());
        $this->assertNotEmpty($totalFilesCount);

        // new token should not have access to any files
        $newTokenClient = $this->getClient([
            'token' => $newToken['token'],
            'url' => STORAGE_API_URL,
        ]);

        $this->assertEmpty($newTokenClient->listFiles());

        $newFileId = $this->createAndWaitForFile($filePath, $uploadOptions, $newTokenClient);
        /** @var array<array{id:int}> $files */
        $files = $newTokenClient->listFiles();
        $this->assertCount(1, $files);
        /** @var array{id:int} $reset */
        $reset = reset($files);
        $this->assertEquals($newFileId, $reset['id']);

        // new file should be visible for master token
        $files = $this->_client->listFiles();
        /** @var array{id:int} $reset */
        $reset = reset($files);
        $this->assertEquals($newFileId, $reset['id']);

        $this->tokens->dropToken($newToken['id']);
    }

    public function testFilesPermissionsCanReadAllFiles(): void
    {
        $uploadOptions = new FileUploadOptions();
        $uploadOptions->setFileName('test.txt')
            ->setFederationToken(true);
        $file = $this->_client->prepareFileUpload($uploadOptions);

        $tokenOptions = (new TokenCreateOptions())
            ->setDescription('Files test')
            ->setCanReadAllFileUploads(true)
        ;

        $newToken = $this->tokens->createToken($tokenOptions);

        // new token should not have access to any files
        $newTokenClient = $this->getClient([
            'token' => $newToken['token'],
            'url' => STORAGE_API_URL,
        ]);

        $file = $newTokenClient->getFile($file['id']);
        $this->assertNotEmpty($file);

        $token = $this->tokens->updateToken(
            (new TokenUpdateOptions($newToken['id']))
                ->setCanReadAllFileUploads(false),
        );

        $this->assertFalse($token['canReadAllFileUploads']);

        try {
            $newTokenClient->getFile($file['id']);
            $this->fail('Access to file should be denied');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals(403, $e->getCode());
            $this->assertEquals('accessDenied', $e->getStringCode());
        }

        $files = $newTokenClient->listFiles();
        $this->assertEmpty($files);
    }

    public function testReadOnlyRoleFilesPermissions(): void
    {
        $expectedError = 'You don\'t have access to the resource.';
        $readOnlyClient = $this->getClientForToken(STORAGE_API_READ_ONLY_TOKEN);

        $options = new FileUploadOptions();
        $fileId = $this->createAndWaitForFile(__DIR__ . '/../_data/files.upload.txt', $options);
        $originalFile = $this->_client->getFile($fileId);
        unset($originalFile['url']);

        $filesCount = count($this->_client->listFiles(new ListFilesOptions()));
        $this->assertGreaterThan(0, $filesCount);

        $this->assertCount($filesCount, $readOnlyClient->listFiles(new ListFilesOptions()));

        try {
            $readOnlyClient->addFileTag($fileId, 'test');
            $this->fail('Files API POST request should be restricted for readOnly user');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('accessDenied', $e->getStringCode());
            $this->assertSame($expectedError, $e->getMessage());
        }

        try {
            $readOnlyClient->deleteFile($fileId);
            $this->fail('Files API DELETE request should be restricted for readOnly user');
        } catch (ClientException $e) {
            $this->assertSame(403, $e->getCode());
            $this->assertSame('accessDenied', $e->getStringCode());
            $this->assertSame($expectedError, $e->getMessage());
        }

        $file = $this->_client->getFile($fileId);
        unset($file['url']);
        $this->assertSame($originalFile, $file);
    }
}
