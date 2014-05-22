package org.ncsu.sys.MKmeansTest;

public class TestInstanceOf {
	public static void main(String[] args){
		superA a = new A();
		a.whoAmI();
		if(a instanceof A)
			System.out.println("Cool!");
		else if(a instanceof superA)
			System.out.println("YOU ARE SCREWED !");
	}
}

class superA{
	public void whoAmI(){
		System.out.println("This is super A!");
	}
}

class A extends superA{
	public void whoAmI(){
		System.out.println("This is A!");
	}
}
