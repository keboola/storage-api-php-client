<?php

declare(strict_types=1);

namespace Keboola\Test\File;

use Keboola\StorageApi\Options\FileUploadOptions;
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
                ->setCanReadAllFileUploads(false)
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
}
