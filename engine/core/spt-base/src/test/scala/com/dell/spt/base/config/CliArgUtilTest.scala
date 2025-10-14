package com.dell.spt.base.config

import java.lang.Boolean.TRUE

import org.junit.Assert.assertEquals
import org.junit.Test
import scala.language.postfixOps

final class CliArgUtilTest {

	@Test @throws[Exception]
	def test(): Unit = {
		val parsedArgs = CliArgUtil parseArgs(
			"--name=goose",
			"--io-buffer-size=1KB-4MB",
			"--enable-some-feature",
		)
		assertEquals("goose", parsedArgs get "name")
		assertEquals("1KB-4MB", parsedArgs get "io-buffer-size")
		assertEquals(TRUE toString, parsedArgs get "enable-some-feature")
	}
}
