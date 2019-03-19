package com.test.spark;

/**
 * 
 * @author vivek
 *
 */
public class TestLocal {

	public static void main(String[] args) {
		String test = String.format("%.2f", 1.23456);
		System.out.println(test);
		printBox();
	}

	public static void printBox()
	{
	    for (int i=0x0;i<=0xFFFF;i++)
	    {
	        System.out.printf("0x%x : %c\n",i,(char)i);
	    }
	    System.out.printf("%c\n",(char)254);
	}
}
