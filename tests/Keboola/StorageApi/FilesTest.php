<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

use \Keboola\StorageApi\Options\FileUploadOptions,
	\Keboola\StorageApi\Options\ListFilesOptions;

class Keboola_StorageApi_FilesTest extends StorageApiTestCase
{



	public function testFileList()
	{
		$options = new FileUploadOptions();
		$options->setFileName(__DIR__ . '/_data/files.upload.txt');
		$fileId = $this->_client->uploadFile($options);
		$files = $this->_client->listFiles(new ListFilesOptions());
		$this->assertNotEmpty($files);
		$this->assertEquals($fileId, reset($files)['id']);
	}

	public function testFilesListFilterByTags()
	{
		$filePath = __DIR__ . '/_data/files.upload.txt';

		$this->_client->uploadFile((new FileUploadOptions())->setFileName($filePath));
		$tag = uniqid('tag-test');
		$fileId = $this->_client->uploadFile((new FileUploadOptions())->setFileName($filePath)->setTags(array($tag)));

		$files = $this->_client->listFiles((new ListFilesOptions())->setTags(array($tag)));

		$this->assertCount(1, $files);
		$file = reset($files);
		$this->assertEquals($fileId, $file['id']);
	}

	/**
	 * @dataProvider uploadData
	 */
	public function testFileUpload(FileUploadOptions $options)
	{
		$fileId = $this->_client->uploadFile($options);
		$file = $this->_client->getFile($fileId);

		$this->assertEquals($options->getIsPublic(), $file['isPublic']);
		$this->assertEquals(basename($options->getFileName()), $file['name']);
		$this->assertEquals(filesize($options->getFileName()), $file['sizeBytes']);
		$this->assertEquals(file_get_contents($options->getFileName()), file_get_contents($file['url']));

		$tags = $options->getTags();
		sort($tags);
		$fileTags = $file['tags'];
		sort($fileTags);
		$this->assertEquals($tags, $fileTags);

		$info = $this->_client->getLogData();
		$this->assertEquals($file['creatorToken']['id'], (int) $info['id']);
		$this->assertEquals($file['creatorToken']['description'], $info['description']);
	}


	/**
	 * @dataProvider uploadData with compress = true
	 */
	public function testFileUploadCompress()
	{
		$filePath = __DIR__ . '/_data/files.upload.txt';
		$fileId = $this->_client->uploadFile((new FileUploadOptions())->setFileName($filePath)->setCompress(true));
		$file = $this->_client->getFile($fileId);

		$this->assertEquals(basename($filePath) . ".gz", $file['name']);

		$gzFile = gzopen($file['url'], "r");
		$this->assertEquals(file_get_contents($filePath), gzread($gzFile, 524288));
	}

	public function uploadData()
	{
		$path  = __DIR__ . '/_data/files.upload.txt';;
		return array(
			array(
				(new FileUploadOptions())
					->setFileName($path)
			),
			array(
				(new FileUploadOptions())
					->setFileName($path)
					->setNotify(false)
					->setCompress(false)
					->setIsPublic(false)
			),
			array(
				(new FileUploadOptions())
					->setFileName($path)
					->setIsPublic(true)
					->setTags(array('sapi-import', 'martin'))
			),
		);
	}



	public function testFilesPermissions()
	{
		$uploadOptions = (new FileUploadOptions())->setFileName( __DIR__ . '/_data/files.upload.txt');

		$newTokenId = $this->_client->createToken(array(), 'Files test');
		$newToken = $this->_client->getToken($newTokenId);
		$firstFileId = $this->_client->uploadFile($uploadOptions);

		$totalFilesCount = count($this->_client->listFiles());
		$this->assertNotEmpty($totalFilesCount);

		// new token should not have access to any files
		$newTokenClient = new Keboola\StorageApi\Client($newToken['token'], STORAGE_API_URL);
		$this->assertEmpty($newTokenClient->listFiles());

		$newFileId = $newTokenClient->uploadFile($uploadOptions);
		$files = $newTokenClient->listFiles();
		$this->assertCount(1, $files);
		$this->assertEquals($newFileId, reset($files)['id']);

		// new file should be visible for master token
		$files = $this->_client->listFiles();
		$this->assertEquals($newFileId, reset($files)['id']);

		$this->_client->dropToken($newTokenId);

		// new token wil all bucket permissions
		$newTokenId = $this->_client->createToken(array(), 'files manage', null, true);
		$newToken = $this->_client->getToken($newTokenId);


		$this->_client->dropToken($newTokenId);
	}

}
