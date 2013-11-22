<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

class Keboola_StorageApi_FilesTest extends StorageApiTestCase
{

	protected $_testFilePath;

	public function setUp()
	{
		parent::setUp();
		$this->_testFilePath = __DIR__ . '/_data/files.upload.txt';
	}

	public function testFileList()
	{
		$initialFilesCount = count($this->_client->listFiles());
		$this->_client->uploadFile($this->_testFilePath);
		$this->assertCount($initialFilesCount + 1, $this->_client->listFiles());
	}

	/**
	 * @dataProvider uploadData
	 */
	public function testFileUpload($isPublic)
	{
		$fileId = $this->_client->uploadFile($this->_testFilePath, $isPublic);
		$file = $this->_client->getFile($fileId);

		$this->assertEquals($isPublic, $file['isPublic']);
		$this->assertEquals(basename($this->_testFilePath), $file['name']);
		$this->assertEquals(filesize($this->_testFilePath), $file['sizeBytes']);
		$this->assertEquals(file_get_contents($this->_testFilePath), file_get_contents($file['url']));

		$info = $this->_client->getLogData();
		$this->assertEquals($file['creatorToken']['id'], (int) $info['id']);
		$this->assertEquals($file['creatorToken']['description'], $info['description']);
	}

	public function uploadData()
	{
		return array(
			array(false),
			array(true),
		);
	}

	public function testFilesPermissions()
	{

		$newTokenId = $this->_client->createToken(array(), 'Files test');
		$newToken = $this->_client->getToken($newTokenId);
		$this->_client->uploadFile($this->_testFilePath);

		$totalFilesCount = count($this->_client->listFiles());
		$this->assertNotEmpty($totalFilesCount);

		// new token should not have access to any files
		$newTokenClient = new Keboola\StorageApi\Client($newToken['token'], STORAGE_API_URL);
		$this->assertEmpty($newTokenClient->listFiles());

		$newTokenClient->uploadFile($this->_testFilePath);
		$this->assertCount(1, $newTokenClient->listFiles());

		// new file should be visible for master token
		$this->assertCount($totalFilesCount + 1, $this->_client->listFiles());

		$this->_client->dropToken($newTokenId);

		// new token wil all bucket permissions
		$newTokenId = $this->_client->createToken(array(), 'files manage', null, true);
		$newToken = $this->_client->getToken($newTokenId);

		// shooul se all files as master token
		$newTokenClient = new Keboola\StorageApi\Client($newToken['token'], STORAGE_API_URL);
		$this->assertCount($totalFilesCount + 1, $newTokenClient->listFiles());

		$this->_client->dropToken($newTokenId);
	}

}