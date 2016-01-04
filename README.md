# Frameless

[![Join the chat at https://gitter.im/adelbertc/frameless](https://badges.gitter.im/adelbertc/frameless.svg)](https://gitter.im/adelbertc/frameless?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Frameless is a way of using [Spark SQL](http://spark.apache.org/sql/) without completely giving up types. The general
idea is having a phantom type that mirrors the value-level computation at the type-level. All heavy lifting is still
done by the original Spark API.

## License
Code is provided under the Apache 2.0 license available at http://opensource.org/licenses/Apache-2.0,
as well as in the LICENSE file. This is the same license used as Spark.
